package com.example.faxdelivery.kafka;

import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.FilterProps;
import com.example.faxdelivery.config.HeaderProps;
import com.example.faxdelivery.logging.HeaderRenderer;

/**
 * PREFERENCE header check. Always the FIRST thing a listener does - before parsing, before waiting for a due
 * time - so records of other flows (EMAIL on the source topic, another flow sharing the retry topic) are
 * acknowledged immediately and can never delay or break fax processing.
 */
@Component
public class PreferenceFilter {

    private final HeaderProps headers;
    private final FilterProps filter;

    public PreferenceFilter(HeaderProps headers, FilterProps filter) {
        this.headers = headers;
        this.filter = filter;
    }

    public boolean matches(Headers recordHeaders) {
        String actual = actual(recordHeaders);
        return actual != null && actual.trim().equalsIgnoreCase(filter.value());
    }

    public String actual(Headers recordHeaders) {
        return HeaderRenderer.value(recordHeaders, headers.preference());
    }

    public String expected() {
        return filter.value();
    }
}
