package com.example.faxdelivery.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param dueWaitSlack         a record is never waited on longer than topic delay + slack (protects against a
 *                             bogus far-future not-before header or clock skew)
 * @param holdBackoff          pause before a held record (breaker open/half-open full) is re-read
 * @param infraBackoffInitial  first back-off after an infrastructure failure (Kafka send/commit)
 * @param infraBackoffMax      cap of that back-off; infrastructure failures retry without limit
 */
@ConfigurationProperties("app.timing")
public record TimingProps(Duration dueWaitSlack, Duration holdBackoff, Duration infraBackoffInitial,
                          Duration infraBackoffMax) {

    public TimingProps {
        dueWaitSlack = dueWaitSlack == null ? Duration.ofSeconds(10) : dueWaitSlack;
        holdBackoff = holdBackoff == null ? Duration.ofSeconds(1) : holdBackoff;
        infraBackoffInitial = infraBackoffInitial == null ? Duration.ofSeconds(1) : infraBackoffInitial;
        infraBackoffMax = infraBackoffMax == null ? Duration.ofSeconds(60) : infraBackoffMax;
    }
}
