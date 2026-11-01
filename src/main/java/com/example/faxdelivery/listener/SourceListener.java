package com.example.faxdelivery.listener;

import java.time.Clock;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.SubmitProps;
import com.example.faxdelivery.flow.Decision;
import com.example.faxdelivery.flow.DecisionExecutor;
import com.example.faxdelivery.flow.SubmitFlow;
import com.example.faxdelivery.kafka.PreferenceFilter;
import com.example.faxdelivery.kafka.TaskHeaders;
import com.example.faxdelivery.logging.HeaderRenderer;
import com.example.faxdelivery.logging.LogContext;
import com.example.faxdelivery.logging.MdcScope;
import com.example.faxdelivery.logging.Outcome;
import com.example.faxdelivery.logging.OutcomeLogger;
import com.example.faxdelivery.resilience.BreakerGate;
import com.example.faxdelivery.task.FaxTask;
import com.example.faxdelivery.task.Origin;

/**
 * Source topic (shared with EMAIL etc.; own consumer group). Filter, then the FIRST submit attempt.
 * <ul>
 *   <li>PREFERENCE != FAX -> FILTERED (DEBUG + counter), committed.</li>
 *   <li>Breaker OPEN -> HOLD: the source record stays unread-committed until the breaker half-opens.</li>
 *   <li>Otherwise SubmitFlow: success -> poll topic, transient -> retry topic, bad data / permanent -> ERROR.</li>
 * </ul>
 * firstSeenAt = the source record's Kafka timestamp, so the submit deadline also covers time spent waiting on
 * the source topic (e.g. while the consumer was paused by the breaker).
 */
@Component
public class SourceListener {

    private final PreferenceFilter filter;
    private final TaskHeaders taskHeaders;
    private final HeaderRenderer headerRenderer;
    private final BreakerGate breakerGate;
    private final SubmitFlow submitFlow;
    private final DecisionExecutor executor;
    private final OutcomeLogger outcomes;
    private final SubmitProps submit;
    private final Clock clock;

    public SourceListener(PreferenceFilter filter, TaskHeaders taskHeaders, HeaderRenderer headerRenderer,
                          BreakerGate breakerGate, SubmitFlow submitFlow, DecisionExecutor executor,
                          OutcomeLogger outcomes, SubmitProps submit, Clock clock) {
        this.filter = filter;
        this.taskHeaders = taskHeaders;
        this.headerRenderer = headerRenderer;
        this.breakerGate = breakerGate;
        this.submitFlow = submitFlow;
        this.executor = executor;
        this.outcomes = outcomes;
        this.submit = submit;
        this.clock = clock;
    }

    @KafkaListener(id = ListenerIds.SOURCE, idIsGroup = false, groupId = "${app.consumers.source.group-id}",
            topics = "${app.topics.source}", containerFactory = ListenerIds.SOURCE_FACTORY,
            concurrency = "${app.consumers.source.concurrency:1}",
            autoStartup = "${app.consumers.source.auto-startup:true}")
    public void onMessage(ConsumerRecord<String, String> record) {
        Map<String, String> headers = headerRenderer.render(record.headers());
        String mid = taskHeaders.mid(record.headers());
        LogContext ctx = LogContext.ofSourceRecord(record, mid, headers);
        try (MdcScope ignored = MdcScope.open(ctx)) {
            if (!filter.matches(record.headers())) {
                outcomes.log(Outcome.FILTERED, ctx, "PREFERENCE=" + filter.actual(record.headers()));
                return;
            }
            if (breakerGate.isOpen()) {
                executor.apply(new Decision.Hold("BREAKER_" + breakerGate.state()), ctx);
            }
            long now = clock.millis();
            long firstSeenAt = record.timestamp() > 0 ? Math.min(record.timestamp(), now) : now;
            FaxTask task = FaxTask.newSubmit(mid, Origin.of(record), headers, record.value(), firstSeenAt,
                    firstSeenAt + submit.maxAge().toMillis(), now);
            executor.apply(submitFlow.run(task, now), ctx.withTask(task));
        }
    }
}
