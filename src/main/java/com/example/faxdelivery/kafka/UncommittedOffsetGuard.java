package com.example.faxdelivery.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

/**
 * With auto.offset.reset=latest, a partition without a committed offset (first deploy, offsets expired after a
 * long full outage) silently starts at the end. This guard logs one ERROR per such partition with the exact
 * offset range that will NOT be processed.
 */
public class UncommittedOffsetGuard implements ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(UncommittedOffsetGuard.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final String listenerId;
    private final String autoOffsetReset;

    public UncommittedOffsetGuard(String listenerId, String autoOffsetReset) {
        this.listenerId = listenerId;
        this.autoOffsetReset = autoOffsetReset;
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(partitions), TIMEOUT);
            List<TopicPartition> missing = partitions.stream().filter(tp -> committed.get(tp) == null).toList();
            if (missing.isEmpty()) {
                return;
            }
            Map<TopicPartition, Long> begin = consumer.beginningOffsets(missing, TIMEOUT);
            Map<TopicPartition, Long> end = consumer.endOffsets(missing, TIMEOUT);
            for (TopicPartition tp : missing) {
                log.error("outcome=NO_COMMITTED_OFFSET listener={} topic={} partition={} autoOffsetReset={} "
                                + "beginningOffset={} endOffset={} detail=\"with 'latest', records in "
                                + "[beginningOffset, endOffset) are not processed\"",
                        listenerId, tp.topic(), tp.partition(), autoOffsetReset, begin.get(tp), end.get(tp));
            }
        }
        catch (RuntimeException e) {
            log.warn("listener={} could not check committed offsets for {}: {}", listenerId, partitions, e.toString());
        }
    }
}
