package com.example.faxdelivery.error;

/** Try again later. Counted by the circuit breaker. */
public class TransientFailureException extends FailureException {

    public TransientFailureException(String reason, String message, Integer httpStatus, boolean ambiguous,
                                     Throwable cause) {
        super(reason, message, httpStatus, ambiguous, cause);
    }
}
