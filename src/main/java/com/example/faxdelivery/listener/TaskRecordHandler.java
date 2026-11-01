package com.example.faxdelivery.listener;

import java.time.Clock;
import java.time.Duration;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.flow.Decision;
import com.example.faxdelivery.flow.DecisionExecutor;
import com.example.faxdelivery.kafka.DueTimeWaiter;
import com.example.faxdelivery.kafka.PreferenceFilter;
import com.example.faxdelivery.kafka.TaskHeaders;
import com.example.faxdelivery.logging.HeaderRenderer;
import com.example.faxdelivery.logging.LogContext;
import com.example.faxdelivery.logging.MdcScope;
import com.example.faxdelivery.logging.Outcome;
import com.example.faxdelivery.logging.OutcomeLogger;
import com.example.faxdelivery.resilience.BreakerGate;
import com.example.faxdelivery.task.FaxTask;
import com.example.faxdelivery.task.InvalidTaskException;
import com.example.faxdelivery.task.TaskCodec;

/**
 * Shared handling of a record on a DELAYED topic (retry or poll). Order matters:
 * <ol>
 *   <li>PREFERENCE filter first - foreign records (another flow sharing the topic) are acked immediately and
 *       never waited on, so they can never delay fax records.</li>
 *   <li>Decode the task (undecodable = INVALID_TASK, terminal).</li>
 *   <li>Wait until due (not-before header), bounded by topic delay + slack.</li>
 *   <li>Breaker OPEN -> HOLD (the container is being paused; the record is read again on HALF_OPEN).</li>
 *   <li>Run the step (pure decision) and apply it (publish + log, or terminal log, or hold).</li>
 * </ol>
 * The offset is committed with the published task in one transaction when this method returns normally.
 */
@Component
public class TaskRecordHandler {

    private final PreferenceFilter filter;
    private final TaskCodec codec;
    private final TaskHeaders taskHeaders;
    private final HeaderRenderer headerRenderer;
    private final DueTimeWaiter dueTimeWaiter;
    private final BreakerGate breakerGate;
    private final DecisionExecutor executor;
    private final OutcomeLogger outcomes;
    private final Clock clock;

    public TaskRecordHandler(PreferenceFilter filter, TaskCodec codec, TaskHeaders taskHeaders,
                             HeaderRenderer headerRenderer, DueTimeWaiter dueTimeWaiter, BreakerGate breakerGate,
                             DecisionExecutor executor, OutcomeLogger outcomes, Clock clock) {
        this.filter = filter;
        this.codec = codec;
        this.taskHeaders = taskHeaders;
        this.headerRenderer = headerRenderer;
        this.dueTimeWaiter = dueTimeWaiter;
        this.breakerGate = breakerGate;
        this.executor = executor;
        this.outcomes = outcomes;
        this.clock = clock;
    }

    public void handle(ConsumerRecord<String, String> record, String listenerId, boolean filterEnabled,
                       Duration topicDelay, BiFunction<FaxTask, Long, Decision> step) {
        if (filterEnabled && !filter.matches(record.headers())) {
            outcomes.log(Outcome.FILTERED, LogContext.ofTaskRecord(record, taskHeaders.mid(record.headers()),
                    headerRenderer.render(record.headers())), "PREFERENCE=" + filter.actual(record.headers()));
            return;
        }
        FaxTask task;
        try {
            task = codec.decode(record.value());
        }
        catch (InvalidTaskException e) {
            outcomes.log(Outcome.INVALID_TASK, LogContext.ofTaskRecord(record, taskHeaders.mid(record.headers()),
                    headerRenderer.render(record.headers())), e.describe());
            return;
        }
        LogContext ctx = LogContext.ofTask(task, record);
        try (MdcScope ignored = MdcScope.open(ctx)) {
            long notBefore = taskHeaders.notBefore(record.headers()).orElse(task.notBefore());
            dueTimeWaiter.awaitDue(notBefore, listenerId, topicDelay);
            if (breakerGate.isOpen()) {
                executor.apply(new Decision.Hold("BREAKER_" + breakerGate.state()), ctx);
            }
            executor.apply(step.apply(task, clock.millis()), ctx);
        }
    }
}
