package com.example.faxdelivery.error;

/** Can never succeed as-is. Terminal: logged ERROR, nothing republished. */
public class PermanentFailureException extends FailureException {

    public PermanentFailureException(String reason, String message) {
        this(reason, message, null, null);
    }

    public PermanentFailureException(String reason, String message, Integer httpStatus, Throwable cause) {
        super(reason, message, httpStatus, false, cause);
    }
}
