package com.example.faxdelivery.error;

/**
 * The circuit breaker did not permit the call (OPEN, or HALF_OPEN with all probe calls in use).
 * Not a failure of the record: the record is HELD (transaction rolled back, re-read later), never requeued.
 * No stack trace: this is control flow.
 */
public class BreakerRejectedException extends RuntimeException {

    public BreakerRejectedException(String operation, Throwable cause) {
        super(operation + ": circuit breaker not permitting calls", cause, false, false);
    }
}
