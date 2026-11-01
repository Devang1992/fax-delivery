package com.example.faxdelivery.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param delay          FIXED delay of every record on the poll topic
 * @param maxAge         no final status by submittedAt + maxAge = FINAL_TIMEOUT; stamped on the task at submit
 * @param maxUnexpected  unclassified exceptions tolerated per job before UNEXPECTED_EXHAUSTED
 */
@ConfigurationProperties("app.poll")
public record PollProps(Duration delay, Duration maxAge, Integer maxUnexpected) {

    public PollProps {
        delay = delay == null ? Duration.ofSeconds(30) : delay;
        maxAge = maxAge == null ? Duration.ofHours(2) : maxAge;
        maxUnexpected = maxUnexpected == null ? 3 : maxUnexpected;
    }
}
