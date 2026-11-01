package com.example.faxdelivery.config;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Kafka connection and transaction settings shared by all fax clients.
 *
 * @param bootstrapServers     broker list
 * @param transactionIdPrefix  MUST be unique per running instance (use the pod name), otherwise two pods fence
 *                             each other's producers
 * @param transactionTimeout   producer transaction.timeout.ms (broker caps it at transaction.max.timeout.ms)
 * @param isolationLevel       read_committed: consumers never see records of aborted transactions
 * @param properties           raw client properties for every client (security.protocol, sasl.*, ssl.*);
 *                             YAML keys with dots must be bracketed: "[security.protocol]": SASL_SSL
 */
@ConfigurationProperties("app.kafka")
public record KafkaProps(String bootstrapServers, String transactionIdPrefix, Duration transactionTimeout,
                         String isolationLevel, Map<String, String> properties) {

    public KafkaProps {
        Objects.requireNonNull(bootstrapServers, "app.kafka.bootstrap-servers is required");
        transactionIdPrefix = blank(transactionIdPrefix) ? "fax-delivery-local-" : transactionIdPrefix;
        transactionTimeout = transactionTimeout == null ? Duration.ofMinutes(5) : transactionTimeout;
        isolationLevel = blank(isolationLevel) ? "read_committed" : isolationLevel;
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }
}
