package com.example.faxdelivery.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.TopicProps;
import com.example.faxdelivery.task.FaxTask;
import com.example.faxdelivery.task.TaskCodec;

/**
 * Publishes the next task. Always called on a listener thread, so the send joins the container's Kafka
 * transaction: the new task and the consumed offset commit together, or neither does. (The template is
 * transactional; a send outside a transaction fails fast instead of silently breaking that guarantee.)
 */
@Component
public class TaskPublisher {

    private final KafkaTemplate<String, String> template;
    private final TaskCodec codec;
    private final TaskHeaders headers;
    private final TopicProps topics;

    public TaskPublisher(KafkaTemplate<String, String> template, TaskCodec codec, TaskHeaders headers,
                         TopicProps topics) {
        this.template = template;
        this.codec = codec;
        this.headers = headers;
        this.topics = topics;
    }

    public void publishRetry(FaxTask task) {
        send(topics.retry(), task);
    }

    public void publishPoll(FaxTask task) {
        send(topics.poll(), task);
    }

    private void send(String topic, FaxTask task) {
        template.send(new ProducerRecord<>(topic, null, null, task.kafkaKey(), codec.encode(task),
                headers.build(task)));
    }
}
