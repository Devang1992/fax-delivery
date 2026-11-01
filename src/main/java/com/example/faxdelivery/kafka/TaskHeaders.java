package com.example.faxdelivery.kafka;

import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.FilterProps;
import com.example.faxdelivery.config.HeaderProps;
import com.example.faxdelivery.logging.HeaderRenderer;
import com.example.faxdelivery.task.FaxTask;

/**
 * Routing headers on every retry/poll record: PREFERENCE (filter), MID (tracing), not-before (due time),
 * stage and task version. A consumer can filter and wait WITHOUT parsing the value.
 */
@Component
public class TaskHeaders {

    private final HeaderProps names;
    private final FilterProps filter;

    public TaskHeaders(HeaderProps names, FilterProps filter) {
        this.names = names;
        this.filter = filter;
    }

    public Headers build(FaxTask task) {
        RecordHeaders headers = new RecordHeaders();
        put(headers, names.preference(), filter.value());
        put(headers, names.mid(), task.mid());
        put(headers, names.notBefore(), Long.toString(task.notBefore()));
        put(headers, names.stage(), task.stage().name());
        put(headers, names.taskVersion(), Integer.toString(task.version()));
        return headers;
    }

    public OptionalLong notBefore(Headers headers) {
        String v = HeaderRenderer.value(headers, names.notBefore());
        if (v == null || v.isBlank()) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(v.trim()));
        }
        catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }

    public String mid(Headers headers) {
        return HeaderRenderer.value(headers, names.mid());
    }

    private static void put(RecordHeaders headers, String name, String value) {
        if (value != null) {
            headers.add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
