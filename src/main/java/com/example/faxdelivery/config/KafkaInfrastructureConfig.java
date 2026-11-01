package com.example.faxdelivery.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import com.example.faxdelivery.error.HoldRecordException;
import com.example.faxdelivery.error.Throwables;
import com.example.faxdelivery.kafka.RollbackRetryListener;
import com.example.faxdelivery.kafka.UncommittedOffsetGuard;
import com.example.faxdelivery.listener.ListenerIds;

/**
 * Kafka wiring.
 * <ul>
 *   <li>Transactional producer + KafkaTransactionManager on every fax container: for each record, the published
 *       next task (retry/poll) and the consumed offset commit atomically (exactly one next task per step).</li>
 *   <li>No CommonErrorHandler on these containers (with a transaction manager the container then rolls back and
 *       hands the record to the after-rollback processor).</li>
 *   <li>After-rollback processor: seek back and retry, never skip. Hold -> short fixed back-off (container is
 *       paused meanwhile); infrastructure failure -> exponential back-off without limit.</li>
 *   <li>read_committed consumers, AckMode.RECORD, cooperative-sticky assignment, one group per listener.</li>
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
@EnableKafka
public class KafkaInfrastructureConfig {

    private static final Logger log = LoggerFactory.getLogger("fax.rollback");

    @Bean
    ProducerFactory<String, String> faxProducerFactory(KafkaProps kafka) {
        Map<String, Object> config = new HashMap<>(kafka.properties());
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) kafka.transactionTimeout().toMillis());
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(config);
        factory.setTransactionIdPrefix(kafka.transactionIdPrefix());
        return factory;
    }

    @Bean
    KafkaTemplate<String, String> faxKafkaTemplate(ProducerFactory<String, String> faxProducerFactory) {
        return new KafkaTemplate<>(faxProducerFactory);
    }

    @Bean
    KafkaTransactionManager<String, String> faxTransactionManager(
            ProducerFactory<String, String> faxProducerFactory) {
        return new KafkaTransactionManager<>(faxProducerFactory);
    }

    @Bean
    DefaultAfterRollbackProcessor<String, String> faxAfterRollbackProcessor(
            KafkaTemplate<String, String> faxKafkaTemplate, TimingProps timing) {
        ExponentialBackOff infra = new ExponentialBackOff(timing.infraBackoffInitial().toMillis(), 2.0);
        infra.setMaxInterval(timing.infraBackoffMax().toMillis());
        DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(
                (record, ex) -> log.error("outcome=UNRECOVERABLE at={}-{}@{} cause=\"{}\" detail=\"back-off "
                                + "exhausted; record skipped\"", record.topic(), record.partition(), record.offset(),
                        ex == null ? "unknown" : Throwables.rootMessage(ex)),
                infra, faxKafkaTemplate, false);
        FixedBackOff hold = new FixedBackOff(timing.holdBackoff().toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS);
        processor.setBackOffFunction((record, ex) ->
                ex != null && Throwables.find(ex, HoldRecordException.class) != null ? hold : null);
        processor.setRetryListeners(new RollbackRetryListener());
        return processor;
    }

    @Bean(ListenerIds.SOURCE_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, String> faxSourceContainerFactory(
            KafkaProps kafka, ConsumerProps consumers, KafkaTransactionManager<String, String> faxTransactionManager,
            DefaultAfterRollbackProcessor<String, String> faxAfterRollbackProcessor) {
        return factory(ListenerIds.SOURCE, consumers.source(), kafka, faxTransactionManager,
                faxAfterRollbackProcessor);
    }

    @Bean(ListenerIds.RETRY_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, String> faxRetryContainerFactory(
            KafkaProps kafka, ConsumerProps consumers, KafkaTransactionManager<String, String> faxTransactionManager,
            DefaultAfterRollbackProcessor<String, String> faxAfterRollbackProcessor) {
        return factory(ListenerIds.RETRY, consumers.retry(), kafka, faxTransactionManager, faxAfterRollbackProcessor);
    }

    @Bean(ListenerIds.POLL_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, String> faxPollContainerFactory(
            KafkaProps kafka, ConsumerProps consumers, KafkaTransactionManager<String, String> faxTransactionManager,
            DefaultAfterRollbackProcessor<String, String> faxAfterRollbackProcessor) {
        return factory(ListenerIds.POLL, consumers.poll(), kafka, faxTransactionManager, faxAfterRollbackProcessor);
    }

    private static ConcurrentKafkaListenerContainerFactory<String, String> factory(
            String listenerId, ConsumerProps.Settings s, KafkaProps kafka,
            KafkaTransactionManager<String, String> transactionManager,
            DefaultAfterRollbackProcessor<String, String> afterRollbackProcessor) {

        Map<String, Object> config = new HashMap<>(kafka.properties());
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, s.groupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, s.autoOffsetReset());
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, s.maxPollRecords());
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) s.maxPollInterval().toMillis());
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, kafka.isolationLevel());
        config.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        factory.setConcurrency(s.concurrency());
        factory.setAutoStartup(s.autoStartup());
        factory.setAfterRollbackProcessor(afterRollbackProcessor);

        ContainerProperties container = factory.getContainerProperties();
        container.setAckMode(ContainerProperties.AckMode.RECORD);
        container.setKafkaAwareTransactionManager(transactionManager);
        container.setShutdownTimeout(s.shutdownTimeout().toMillis());
        container.setConsumerRebalanceListener(new UncommittedOffsetGuard(listenerId, s.autoOffsetReset()));
        return factory;
    }
}
