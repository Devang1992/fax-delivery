package com.example.faxdelivery.kafka;

import java.time.Clock;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.TimingProps;
import com.example.faxdelivery.error.HoldRecordException;

/**
 * Waits on the consumer thread until a record is due.
 *
 * Why this is correct: every record on a delayed topic has the SAME delay, so records arrive ordered by due
 * time - when the record in hand is not due, nothing behind it on that partition is due either. Waiting here
 * is therefore equivalent to pausing the partition, without pause bookkeeping.
 *
 * Bounds: never longer than topic delay + slack (guards against a bogus header / clock skew); the sleep wakes
 * up when the container stops; a stop during the wait HOLDS the record (rolled back, read again later).
 * Requirement: concurrency >= partitions per instance, so one thread never waits on behalf of two partitions
 * (if it does, the other partition is only delayed, never lost).
 */
@Component
public class DueTimeWaiter {

    private static final Logger log = LoggerFactory.getLogger(DueTimeWaiter.class);

    private final KafkaListenerEndpointRegistry registry;
    private final TimingProps timing;
    private final Clock clock;

    public DueTimeWaiter(KafkaListenerEndpointRegistry registry, TimingProps timing, Clock clock) {
        this.registry = registry;
        this.timing = timing;
        this.clock = clock;
    }

    public void awaitDue(long notBefore, String listenerId, Duration topicDelay) {
        long wait = notBefore - clock.millis();
        if (wait <= 0) {
            return;
        }
        long cap = topicDelay.plus(timing.dueWaitSlack()).toMillis();
        if (wait > cap) {
            log.warn("listener={} not-before is {} ms ahead, more than topic delay + slack; waiting {} ms only",
                    listenerId, wait, cap);
            wait = cap;
        }
        MessageListenerContainer container = registry.getListenerContainer(listenerId);
        try {
            if (container != null) {
                ListenerUtils.stoppableSleep(container, wait);
            }
            else {
                Thread.sleep(wait);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HoldRecordException("INTERRUPTED_WHILE_WAITING_FOR_DUE_TIME");
        }
        if (container != null && !container.isRunning()) {
            throw new HoldRecordException("CONTAINER_STOPPING");
        }
    }
}
