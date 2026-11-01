package com.example.faxdelivery.config;

import java.util.Objects;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param source existing topic with all preference messages (EMAIL, FAX, ...)
 * @param retry  delayed SUBMIT retries; may later be shared with another flow (fax consumer filters on PREFERENCE)
 * @param poll   delayed RightFax status checks (fax only)
 */
@ConfigurationProperties("app.topics")
public record TopicProps(String source, String retry, String poll) {

    public TopicProps {
        Objects.requireNonNull(source, "app.topics.source is required");
        Objects.requireNonNull(retry, "app.topics.retry is required");
        Objects.requireNonNull(poll, "app.topics.poll is required");
    }
}
