package com.example.faxdelivery.flow;

import com.example.faxdelivery.logging.Outcome;
import com.example.faxdelivery.task.FaxTask;

/**
 * What a step decided. Flows only DECIDE (pure, testable); {@link DecisionExecutor} applies the decision.
 * Exactly one of: publish a retry task, publish a poll task, end (terminal outcome), or hold the record.
 */
public sealed interface Decision {

    /** Publish to the retry topic (SUBMIT again at task.notBefore). */
    record PublishRetry(FaxTask task, Outcome outcome, String detail) implements Decision {
    }

    /** Publish to the poll topic (check status at task.notBefore). */
    record PublishPoll(FaxTask task, Outcome outcome, String detail) implements Decision {
    }

    /** Done. Nothing is published; one terminal line is logged. */
    record Terminal(Outcome outcome, String detail, Throwable cause) implements Decision {

        public Terminal(Outcome outcome, String detail) {
            this(outcome, detail, null);
        }
    }

    /** Breaker not permitting calls: roll back, keep the record where it is, read it again later. */
    record Hold(String reason) implements Decision {
    }
}
