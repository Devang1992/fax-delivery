package com.example.faxdelivery.resilience;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.faxdelivery.config.BreakerProps;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;

/** Resilience4j core, wired programmatically (no Boot starter dependency, no AOP proxies). */
@Configuration(proxyBeanMethods = false)
public class ResilienceConfig {

    @Bean
    CircuitBreakerRegistry circuitBreakerRegistry() {
        return CircuitBreakerRegistry.ofDefaults();
    }

    @Bean
    CircuitBreaker rightFaxCircuitBreaker(CircuitBreakerRegistry registry, BreakerProps props) {
        return BreakerFactory.create(registry, props);
    }

    /** Exports resilience4j_circuitbreaker_state{name=rightfax,...}, failure/slow rates, call counts. */
    @Bean
    TaggedCircuitBreakerMetrics circuitBreakerMetrics(CircuitBreakerRegistry registry) {
        return TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry);
    }
}
