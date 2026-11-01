package com.example.faxdelivery.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.FilterProps;
import com.example.faxdelivery.config.SubmitProps;
import com.example.faxdelivery.flow.SubmitFlow;

/**
 * Retry topic (fixed delay = app.submit.retry-delay). May be shared with another flow later: the PREFERENCE
 * filter runs first, so foreign records are acked at once and never waited on.
 */
@Component
public class RetryListener {

    private final TaskRecordHandler handler;
    private final SubmitFlow submitFlow;
    private final SubmitProps submit;
    private final FilterProps filter;

    public RetryListener(TaskRecordHandler handler, SubmitFlow submitFlow, SubmitProps submit, FilterProps filter) {
        this.handler = handler;
        this.submitFlow = submitFlow;
        this.submit = submit;
        this.filter = filter;
    }

    @KafkaListener(id = ListenerIds.RETRY, idIsGroup = false, groupId = "${app.consumers.retry.group-id}",
            topics = "${app.topics.retry}", containerFactory = ListenerIds.RETRY_FACTORY,
            concurrency = "${app.consumers.retry.concurrency:1}",
            autoStartup = "${app.consumers.retry.auto-startup:true}")
    public void onMessage(ConsumerRecord<String, String> record) {
        handler.handle(record, ListenerIds.RETRY, filter.onRetryTopic(), submit.retryDelay(), submitFlow::run);
    }
}
