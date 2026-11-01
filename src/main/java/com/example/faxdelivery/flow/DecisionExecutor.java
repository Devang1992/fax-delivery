package com.example.faxdelivery.flow;

import org.springframework.stereotype.Component;

import com.example.faxdelivery.error.HoldRecordException;
import com.example.faxdelivery.kafka.TaskPublisher;
import com.example.faxdelivery.logging.LogContext;
import com.example.faxdelivery.logging.Outcome;
import com.example.faxdelivery.logging.OutcomeLogger;

/**
 * Applies a decision on the listener thread, inside the container's Kafka transaction:
 * publish (joins the transaction) + log, or log only, or HOLD (throws -> rollback -> record read again).
 */
@Component
public class DecisionExecutor {

    private final TaskPublisher publisher;
    private final OutcomeLogger outcomes;

    public DecisionExecutor(TaskPublisher publisher, OutcomeLogger outcomes) {
        this.publisher = publisher;
        this.outcomes = outcomes;
    }

    public void apply(Decision decision, LogContext ctx) {
        switch (decision) {
            case Decision.PublishRetry r -> {
                publisher.publishRetry(r.task());
                outcomes.log(r.outcome(), ctx.withTask(r.task()), r.detail());
            }
            case Decision.PublishPoll p -> {
                publisher.publishPoll(p.task());
                outcomes.log(p.outcome(), ctx.withTask(p.task()), p.detail());
            }
            case Decision.Terminal t -> outcomes.log(t.outcome(), ctx, t.detail(), t.cause());
            case Decision.Hold h -> {
                outcomes.log(Outcome.HELD, ctx, h.reason());
                throw new HoldRecordException(h.reason());
            }
        }
    }
}
