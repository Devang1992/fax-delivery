package com.example.faxdelivery.config;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param maskedHeaders        header names (case-insensitive) logged as ****
 * @param maxHeaderValueLength longer header values are truncated in logs
 * @param maxDetailLength      longer outcome details (e.g. response bodies) are truncated in logs
 */
@ConfigurationProperties("app.logging")
public record LoggingProps(Set<String> maskedHeaders, Integer maxHeaderValueLength, Integer maxDetailLength) {

    public LoggingProps {
        maskedHeaders = maskedHeaders == null ? Set.of("authorization", "cookie", "set-cookie")
                : maskedHeaders.stream().map(h -> h.toLowerCase(Locale.ROOT)).collect(Collectors.toUnmodifiableSet());
        maxHeaderValueLength = maxHeaderValueLength == null ? 512 : maxHeaderValueLength;
        maxDetailLength = maxDetailLength == null ? 2000 : maxDetailLength;
    }
}
