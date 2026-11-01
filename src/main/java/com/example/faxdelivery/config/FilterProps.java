package com.example.faxdelivery.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param value        PREFERENCE header value this service handles (compared trimmed, case-insensitive)
 * @param onRetryTopic filter the retry topic too (required once another flow shares it)
 * @param onPollTopic  filter the poll topic too (cheap safety; poll is fax-only today)
 */
@ConfigurationProperties("app.filter")
public record FilterProps(String value, Boolean onRetryTopic, Boolean onPollTopic) {

    public FilterProps {
        value = value == null || value.isBlank() ? "FAX" : value;
        onRetryTopic = onRetryTopic == null || onRetryTopic;
        onPollTopic = onPollTopic == null || onPollTopic;
    }
}
