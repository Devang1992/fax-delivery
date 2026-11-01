package com.example.faxdelivery.logging;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.example.faxdelivery.task.FaxTask;

/**
 * What every outcome line carries.
 *
 * @param origin  source record coordinates (topic-partition@offset) - links every line of one fax
 * @param at      coordinates of the record being processed now (source, retry or poll topic)
 * @param headers ORIGINAL source headers (rendered, masked)
 */
public record LogContext(String mid, String origin, String at, String key, String stage, int attempt,
                         int pollCount, String jobId, Map<String, String> headers) {

    public LogContext {
        mid = mid == null || mid.isBlank() ? "n/a" : mid;
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    public static LogContext ofSourceRecord(ConsumerRecord<?, ?> record, String mid, Map<String, String> headers) {
        String coords = coordinates(record);
        return new LogContext(mid, coords, coords, record.key() == null ? null : record.key().toString(), "SOURCE",
                0, 0, null, headers);
    }

    public static LogContext ofTaskRecord(ConsumerRecord<?, ?> record, String mid, Map<String, String> headers) {
        String coords = coordinates(record);
        return new LogContext(mid, "unknown", coords, record.key() == null ? null : record.key().toString(),
                "UNKNOWN", 0, 0, null, headers);
    }

    public static LogContext ofTask(FaxTask task, ConsumerRecord<?, ?> record) {
        return new LogContext(task.mid(), task.origin().coordinates(), coordinates(record), task.origin().key(),
                task.stage().name(), task.attempt(), task.pollCount(), task.jobId(), task.originHeaders());
    }

    /** Same record, progress fields taken from the task the step produced. */
    public LogContext withTask(FaxTask task) {
        return new LogContext(mid, origin, at, key, task.stage().name(), task.attempt(), task.pollCount(),
                task.jobId(), headers);
    }

    private static String coordinates(ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "@" + record.offset();
    }
}
