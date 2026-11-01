package com.example.faxdelivery.task;

import com.example.faxdelivery.error.PermanentFailureException;

/** A record on the retry/poll topic that cannot be decoded as a FaxTask (or has an unknown version). */
public class InvalidTaskException extends PermanentFailureException {

    public InvalidTaskException(String message, Throwable cause) {
        super("INVALID_TASK", message, null, cause);
    }
}
