package com.example.faxdelivery.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.BreakerProps;
import com.example.faxdelivery.listener.ListenerIds;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

/**
 * The breaker drives ALL fax consumers (source, retry, poll):
 * <pre>
 * CLOSED ──failures──▶ OPEN ──────────────▶ pause() every fax container (poll keeps heartbeating, no records,
 *                       │                     no rebalance; the record that hit the open breaker was HELD)
 *                       │ wait-duration-in-open-state (automatic)
 *                       ▼
 *                   HALF_OPEN ────────────▶ resume() every container; the next RightFax call(s) are the probe
 *                    │      │
 *        probe ok ◀──┘      └──▶ probe failed: OPEN again (pause again)
 *            ▼
 *         CLOSED ─────────────────────────▶ resume() (no-op if running)
 * </pre>
 * Container pause and the per-record due-time wait do not interfere (the wait runs on the consumer thread;
 * Spring Kafka also keeps partition-level pauses separate from container pauses).
 */
@Component
public class BreakerConsumerControl {

    private static final Logger log = LoggerFactory.getLogger("fax.circuit");

    private final KafkaListenerEndpointRegistry registry;

    public BreakerConsumerControl(@Qualifier("rightFaxCircuitBreaker") CircuitBreaker breaker,
                                  KafkaListenerEndpointRegistry registry, BreakerProps props) {
        this.registry = registry;
        if (!props.pauseConsumersWhenOpen()) {
            log.warn("app.circuit-breaker.pause-consumers-when-open=false: consumers keep polling while OPEN; "
                    + "records are held one by one instead");
            return;
        }
        breaker.getEventPublisher().onStateTransition(event -> {
            switch (event.getStateTransition().getToState()) {
                case OPEN, FORCED_OPEN -> pauseAll();
                case HALF_OPEN, CLOSED, DISABLED, METRICS_ONLY -> resumeAll();
                default -> {
                }
            }
        });
    }

    void pauseAll() {
        for (String id : ListenerIds.ALL) {
            MessageListenerContainer c = registry.getListenerContainer(id);
            if (c != null && !c.isPauseRequested()) {
                c.pause();
                log.error("outcome=CONSUMER_PAUSED listener={} detail=\"RightFax breaker open\"", id);
            }
        }
    }

    void resumeAll() {
        for (String id : ListenerIds.ALL) {
            MessageListenerContainer c = registry.getListenerContainer(id);
            if (c != null && c.isPauseRequested()) {
                c.resume();
                log.warn("outcome=CONSUMER_RESUMED listener={}", id);
            }
        }
    }
}
