package com.example.faxdelivery.logging;

import org.slf4j.event.Level;

/**
 * Every source record ends in exactly ONE terminal outcome. Non-terminal outcomes describe progress.
 * There is no DLQ: terminal ERROR lines ARE the dead-letter record (MID, origin offset, all original headers).
 */
public enum Outcome {
    FILTERED(Level.DEBUG, true),
    INVALID_MESSAGE(Level.ERROR, true),
    INVALID_TASK(Level.ERROR, true),
    HELD(Level.WARN, false),
    SUBMITTED(Level.INFO, false),
    RETRY_SCHEDULED(Level.WARN, false),
    POSSIBLE_DUPLICATE(Level.WARN, false),
    SUBMIT_EXPIRED(Level.ERROR, true),
    SUBMISSION_UNCONFIRMED(Level.ERROR, true),
    FAILED_PERMANENT(Level.ERROR, true),
    FAILED_EXHAUSTED(Level.ERROR, true),
    UNEXPECTED_EXHAUSTED(Level.ERROR, true),
    STATUS_PENDING(Level.DEBUG, false),
    STATUS_CHECK_RETRY(Level.WARN, false),
    DELIVERED(Level.INFO, true),
    FAILED_FINAL_STATUS(Level.ERROR, true),
    FINAL_TIMEOUT(Level.ERROR, true),
    FINAL_TIMEOUT_UNREACHABLE(Level.ERROR, true),
    STATUS_CHECK_FAILED(Level.ERROR, true);

    private final Level level;
    private final boolean terminal;

    Outcome(Level level, boolean terminal) {
        this.level = level;
        this.terminal = terminal;
    }

    public Level level() {
        return level;
    }

    public boolean terminal() {
        return terminal;
    }
}
