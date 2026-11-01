package com.example.faxdelivery.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * SUBMIT = render + PDF + upload + send job.
 *
 * @param retryDelay              FIXED delay of every record on the retry topic (one delay per topic keeps the
 *                                topic ordered by due time)
 * @param maxAge                  give up when firstSeenAt + maxAge has passed (SUBMIT_EXPIRED); business value
 * @param maxUnexpected           unclassified exceptions (bugs) tolerated per message before UNEXPECTED_EXHAUSTED
 * @param maxRetriesOnServerError -1 = retry HTTP 500 until max-age; N = give up after N (FAILED_EXHAUSTED)
 */
@ConfigurationProperties("app.submit")
public record SubmitProps(Duration retryDelay, Duration maxAge, Integer maxUnexpected,
                          Integer maxRetriesOnServerError) {

    public SubmitProps {
        retryDelay = retryDelay == null ? Duration.ofMinutes(1) : retryDelay;
        maxAge = maxAge == null ? Duration.ofHours(4) : maxAge;
        maxUnexpected = maxUnexpected == null ? 3 : maxUnexpected;
        maxRetriesOnServerError = maxRetriesOnServerError == null ? -1 : maxRetriesOnServerError;
    }
}
