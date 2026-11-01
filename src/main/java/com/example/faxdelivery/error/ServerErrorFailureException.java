package com.example.faxdelivery.error;

/** HTTP 500 - the one transient failure that could be caused by the payload, so its retries can be capped. */
public class ServerErrorFailureException extends TransientFailureException {

    public ServerErrorFailureException(String reason, String message, Integer httpStatus, Throwable cause) {
        super(reason, message, httpStatus, true, cause);
    }
}
