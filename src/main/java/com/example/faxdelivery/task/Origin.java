package com.example.faxdelivery.task;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Coordinates of the ORIGINAL source record; carried by every task so any log line can be traced/replayed. */
public record Origin(String topic, int partition, long offset, String key, long timestamp) {

    public static Origin of(ConsumerRecord<?, ?> record) {
        return new Origin(record.topic(), record.partition(), record.offset(),
                record.key() == null ? null : record.key().toString(), record.timestamp());
    }

    public String coordinates() {
        return topic + "-" + partition + "@" + offset;
    }
}
