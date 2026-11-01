package com.example.faxdelivery.resilience;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

/** Read-only view of the RightFax breaker for listeners: "may this record call RightFax now?". */
@Component
public class BreakerGate {

    private final CircuitBreaker breaker;

    public BreakerGate(@Qualifier("rightFaxCircuitBreaker") CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    /** OPEN or FORCED_OPEN: no call can succeed; hold the record. HALF_OPEN is "maybe" and is let through. */
    public boolean isOpen() {
        CircuitBreaker.State s = breaker.getState();
        return s == CircuitBreaker.State.OPEN || s == CircuitBreaker.State.FORCED_OPEN;
    }

    public CircuitBreaker.State state() {
        return breaker.getState();
    }
}
