package com.example.faxdelivery.error;

/**
 * 401 on a call that carried the rf-session cookie: the session expired. The session manager logs in again
 * and repeats the call once. Ignored by the breaker (routine expiry must not open it).
 */
public class SessionExpiredException extends SystemicFailureException {

    public SessionExpiredException(String operation, Throwable cause) {
        super("SESSION_EXPIRED", operation + ": session rejected (401)", 401, cause);
    }
}
