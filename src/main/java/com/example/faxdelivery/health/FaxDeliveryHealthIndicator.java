package com.example.faxdelivery.health;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.listener.ListenerIds;
import com.example.faxdelivery.resilience.BreakerGate;

/**
 * Informational: always UP, with breaker state and every container's running/paused state as details.
 * Deliberately NOT in liveness/readiness - restarting pods cannot fix RightFax and only causes rebalances.
 */
@Component("faxDelivery")
public class FaxDeliveryHealthIndicator implements HealthIndicator {

    private final BreakerGate breaker;
    private final KafkaListenerEndpointRegistry registry;

    public FaxDeliveryHealthIndicator(BreakerGate breaker, KafkaListenerEndpointRegistry registry) {
        this.breaker = breaker;
        this.registry = registry;
    }

    @Override
    public Health health() {
        Map<String, Object> containers = new LinkedHashMap<>();
        for (String id : ListenerIds.ALL) {
            MessageListenerContainer c = registry.getListenerContainer(id);
            containers.put(id, c == null ? "absent"
                    : Map.of("running", c.isRunning(), "pauseRequested", c.isPauseRequested(),
                            "paused", c.isContainerPaused()));
        }
        return Health.up()
                .withDetail("circuitBreaker", breaker.state().name())
                .withDetail("containers", containers)
                .build();
    }
}
