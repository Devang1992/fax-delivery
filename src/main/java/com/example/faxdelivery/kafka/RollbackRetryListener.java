package com.example.faxdelivery.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.RetryListener;

import com.example.faxdelivery.error.HoldRecordException;
import com.example.faxdelivery.error.Throwables;

/**
 * Logs every rolled-back delivery seen by the after-rollback processor. Holds (breaker open, stopping) are
 * expected and logged at DEBUG; anything else is an infrastructure failure (Kafka send/commit) at WARN.
 */
public class RollbackRetryListener implements RetryListener {

    private static final Logger log = LoggerFactory.getLogger("fax.rollback");

    @Override
    public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
        String at = record.topic() + "-" + record.partition() + "@" + record.offset();
        HoldRecordException hold = ex == null ? null : Throwables.find(ex, HoldRecordException.class);
        if (hold != null) {
            log.debug("outcome=HELD_RETRY at={} attempt={} reason={}", at, deliveryAttempt, hold.getMessage());
        }
        else {
            log.warn("outcome=ROLLED_BACK at={} attempt={} cause=\"{}\"", at, deliveryAttempt,
                    ex == null ? "unknown" : Throwables.rootMessage(ex));
        }
    }
}
