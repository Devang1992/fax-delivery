package com.example.faxdelivery.resilience;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.faxdelivery.config.BreakerProps;
import com.example.faxdelivery.error.PermanentFailureException;
import com.example.faxdelivery.error.SessionExpiredException;
import com.example.faxdelivery.error.SystemicFailureException;
import com.example.faxdelivery.error.TransientFailureException;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

/**
 * Builds the RightFax breaker.
 * <ul>
 *   <li>Counts as failure: transient (incl. 500, timeouts, ambiguous) and systemic (401 on login, 403).</li>
 *   <li>Ignored: permanent (bad data never opens the breaker) and the first 401 of an expired session.</li>
 *   <li>Slow calls also count (a crawling RightFax opens the breaker instead of eating poll budgets).</li>
 *   <li>Automatic OPEN -> HALF_OPEN is mandatory: consumers are paused while OPEN, so no call would ever
 *       trigger the transition.</li>
 * </ul>
 */
public final class BreakerFactory {

    private BreakerFactory() {
    }

    public static CircuitBreaker create(CircuitBreakerRegistry registry, BreakerProps p) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(p.slidingWindowSize())
                .minimumNumberOfCalls(p.minimumNumberOfCalls())
                .failureRateThreshold(p.failureRateThreshold())
                .slowCallDurationThreshold(p.slowCallDurationThreshold())
                .slowCallRateThreshold(p.slowCallRateThreshold())
                .waitDurationInOpenState(p.waitDurationInOpenState())
                .permittedNumberOfCallsInHalfOpenState(p.permittedCallsInHalfOpenState())
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .ignoreException(t -> t instanceof PermanentFailureException || t instanceof SessionExpiredException)
                .recordException(t -> t instanceof TransientFailureException || t instanceof SystemicFailureException)
                .build();
        CircuitBreaker breaker = registry.circuitBreaker(p.name(), config);
        attachTransitionLogging(breaker, p.flappingOpensPerHour());
        return breaker;
    }

    private static void attachTransitionLogging(CircuitBreaker breaker, int flappingOpensPerHour) {
        Logger log = LoggerFactory.getLogger("fax.circuit");
        Deque<Instant> opens = new ArrayDeque<>();
        breaker.getEventPublisher().onStateTransition(event -> {
            CircuitBreaker.State from = event.getStateTransition().getFromState();
            CircuitBreaker.State to = event.getStateTransition().getToState();
            if (to == CircuitBreaker.State.OPEN || to == CircuitBreaker.State.FORCED_OPEN) {
                int lastHour;
                synchronized (opens) {
                    Instant now = Instant.now();
                    opens.addLast(now);
                    while (!opens.isEmpty() && opens.peekFirst().isBefore(now.minus(Duration.ofHours(1)))) {
                        opens.removeFirst();
                    }
                    lastHour = opens.size();
                }
                log.error("outcome=CIRCUIT_OPEN breaker={} from={} failureRate={} slowCallRate={} opensLastHour={}",
                        breaker.getName(), from, breaker.getMetrics().getFailureRate(),
                        breaker.getMetrics().getSlowCallRate(), lastHour);
                if (lastHour > flappingOpensPerHour) {
                    log.error("outcome=CIRCUIT_FLAPPING breaker={} opensLastHour={} threshold={}", breaker.getName(),
                            lastHour, flappingOpensPerHour);
                }
            }
            else if (to == CircuitBreaker.State.HALF_OPEN) {
                log.warn("outcome=CIRCUIT_HALF_OPEN breaker={} from={} detail=\"probe calls allowed\"",
                        breaker.getName(), from);
            }
            else {
                log.info("outcome=CIRCUIT_{} breaker={} from={}", to, breaker.getName(), from);
            }
        });
    }
}
