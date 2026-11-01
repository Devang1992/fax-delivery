package com.example.faxdelivery.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.FilterProps;
import com.example.faxdelivery.config.PollProps;
import com.example.faxdelivery.flow.PollFlow;

/** Poll topic (fixed delay = app.poll.delay). One record = one status check of one job. */
@Component
public class PollListener {

    private final TaskRecordHandler handler;
    private final PollFlow pollFlow;
    private final PollProps poll;
    private final FilterProps filter;

    public PollListener(TaskRecordHandler handler, PollFlow pollFlow, PollProps poll, FilterProps filter) {
        this.handler = handler;
        this.pollFlow = pollFlow;
        this.poll = poll;
        this.filter = filter;
    }

    @KafkaListener(id = ListenerIds.POLL, idIsGroup = false, groupId = "${app.consumers.poll.group-id}",
            topics = "${app.topics.poll}", containerFactory = ListenerIds.POLL_FACTORY,
            concurrency = "${app.consumers.poll.concurrency:1}",
            autoStartup = "${app.consumers.poll.auto-startup:true}")
    public void onMessage(ConsumerRecord<String, String> record) {
        handler.handle(record, ListenerIds.POLL, filter.onPollTopic(), poll.delay(), pollFlow::run);
    }
}
