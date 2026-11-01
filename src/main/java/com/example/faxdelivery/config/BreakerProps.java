package com.example.faxdelivery.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * One breaker for every RightFax call (login, upload, send job, job status, documents): same server.
 *
 * @param pauseConsumersWhenOpen pause all fax listeners while OPEN, resume on HALF_OPEN (default true)
 */
@ConfigurationProperties("app.circuit-breaker")
public record BreakerProps(String name, Float failureRateThreshold, Integer slidingWindowSize,
                           Integer minimumNumberOfCalls, Duration waitDurationInOpenState,
                           Integer permittedCallsInHalfOpenState, Duration slowCallDurationThreshold,
                           Float slowCallRateThreshold, Integer flappingOpensPerHour,
                           Boolean pauseConsumersWhenOpen) {

    public BreakerProps {
        name = name == null || name.isBlank() ? "rightfax" : name;
        failureRateThreshold = failureRateThreshold == null ? 50f : failureRateThreshold;
        slidingWindowSize = slidingWindowSize == null ? 10 : slidingWindowSize;
        minimumNumberOfCalls = minimumNumberOfCalls == null ? 5 : minimumNumberOfCalls;
        waitDurationInOpenState = waitDurationInOpenState == null ? Duration.ofSeconds(60) : waitDurationInOpenState;
        permittedCallsInHalfOpenState = permittedCallsInHalfOpenState == null ? 1 : permittedCallsInHalfOpenState;
        slowCallDurationThreshold = slowCallDurationThreshold == null ? Duration.ofSeconds(20)
                : slowCallDurationThreshold;
        slowCallRateThreshold = slowCallRateThreshold == null ? 80f : slowCallRateThreshold;
        flappingOpensPerHour = flappingOpensPerHour == null ? 6 : flappingOpensPerHour;
        pauseConsumersWhenOpen = pauseConsumersWhenOpen == null || pauseConsumersWhenOpen;
    }
}
