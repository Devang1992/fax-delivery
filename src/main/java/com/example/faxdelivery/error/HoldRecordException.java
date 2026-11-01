package com.example.faxdelivery.error;

/**
 * Thrown by a listener to HOLD the current record: the Kafka transaction rolls back, the offset is not
 * committed, the after-rollback processor seeks back (short fixed back-off) and the record is read again
 * once the consumer is resumed. Used for: breaker OPEN / HALF_OPEN rejection, container stopping while
 * waiting for a due time. No stack trace: this is control flow, not an error.
 */
public class HoldRecordException extends RuntimeException {

    public HoldRecordException(String reason) {
        super(reason, null, false, false);
    }
}
