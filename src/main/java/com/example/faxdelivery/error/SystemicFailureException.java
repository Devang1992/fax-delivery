package com.example.faxdelivery.error;

/** Credentials / permissions / setup broken. Retried (never dropped) and counted by the breaker. */
public class SystemicFailureException extends FailureException {

    public SystemicFailureException(String reason, String message, Integer httpStatus, Throwable cause) {
        super(reason, message, httpStatus, false, cause);
    }
}
