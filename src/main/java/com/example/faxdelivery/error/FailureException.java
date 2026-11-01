package com.example.faxdelivery.error;

/**
 * Base of the failure taxonomy. Every failure that leaves a RightFax call or a processing step is one of:
 * <ul>
 *   <li>{@link PermanentFailureException} - can never succeed as-is (bad data, 4xx). Terminal.</li>
 *   <li>{@link TransientFailureException} - try again later; {@link ServerErrorFailureException} is the
 *       HTTP 500 sub-case whose retries can be capped.</li>
 *   <li>{@link SystemicFailureException} - auth/config broken (401/403). Retried, never dropped, counted by
 *       the breaker so a broken credential pauses the consumers instead of dropping faxes.</li>
 * </ul>
 * {@code ambiguous}: the request may already have been processed (response timeout, reset, 500/502/504).
 * Retrying it can duplicate a fax - accepted by the business, logged as POSSIBLE_DUPLICATE.
 */
public abstract class FailureException extends RuntimeException {

    private final String reason;
    private final Integer httpStatus;
    private final boolean ambiguous;

    protected FailureException(String reason, String message, Integer httpStatus, boolean ambiguous, Throwable cause) {
        super(message, cause);
        this.reason = reason;
        this.httpStatus = httpStatus;
        this.ambiguous = ambiguous;
    }

    public String reason() {
        return reason;
    }

    public Integer httpStatus() {
        return httpStatus;
    }

    public boolean isAmbiguous() {
        return ambiguous;
    }

    /** One-line description for logs and for the task's lastError. */
    public String describe() {
        return "reason=" + reason + (httpStatus != null ? " httpStatus=" + httpStatus : "")
                + (ambiguous ? " ambiguous=true" : "") + " message=\"" + getMessage() + "\"";
    }
}
