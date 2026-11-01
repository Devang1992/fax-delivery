package com.example.faxdelivery.config;

import java.time.Duration;
import java.util.Objects;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** One block per listener; each listener has its own consumer group. */
@ConfigurationProperties("app.consumers")
public record ConsumerProps(Settings source, Settings retry, Settings poll) {

    public ConsumerProps {
        Objects.requireNonNull(source, "app.consumers.source is required");
        Objects.requireNonNull(retry, "app.consumers.retry is required");
        Objects.requireNonNull(poll, "app.consumers.poll is required");
    }

    /**
     * @param groupId          consumer group (one per listener)
     * @param concurrency      consumer threads per instance; for retry/poll keep it >= partitions per instance,
     *                         because waiting for a not-yet-due record blocks its thread
     * @param maxPollRecords   small on purpose: bounds the work done between two polls
     * @param maxPollInterval  validated at startup against delay + max-poll-records x worst step time
     * @param autoOffsetReset  for a brand-new group: latest (default) or earliest
     * @param autoStartup      start the listener with the application
     * @param shutdownTimeout  let the in-flight record finish and commit on shutdown
     */
    public record Settings(String groupId, Integer concurrency, Integer maxPollRecords, Duration maxPollInterval,
                           String autoOffsetReset, Boolean autoStartup, Duration shutdownTimeout) {

        public Settings {
            Objects.requireNonNull(groupId, "group-id is required for every consumer");
            concurrency = concurrency == null ? 1 : concurrency;
            maxPollRecords = maxPollRecords == null ? 1 : maxPollRecords;
            maxPollInterval = maxPollInterval == null ? Duration.ofMinutes(10) : maxPollInterval;
            autoOffsetReset = autoOffsetReset == null || autoOffsetReset.isBlank() ? "latest" : autoOffsetReset;
            autoStartup = autoStartup == null || autoStartup;
            shutdownTimeout = shutdownTimeout == null ? Duration.ofMinutes(3) : shutdownTimeout;
        }
    }
}
