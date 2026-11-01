package com.example.faxdelivery.logging;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.LoggingProps;

/** Renders all headers for logs: UTF-8, control chars replaced, masked names, truncated values. */
@Component
public class HeaderRenderer {

    private final LoggingProps props;

    public HeaderRenderer(LoggingProps props) {
        this.props = props;
    }

    public Map<String, String> render(Headers headers) {
        Map<String, String> out = new LinkedHashMap<>();
        if (headers == null) {
            return out;
        }
        for (Header h : headers) {
            String value = props.maskedHeaders().contains(h.key().toLowerCase(Locale.ROOT)) ? "****"
                    : decode(h.value());
            out.merge(h.key(), value, (a, b) -> a + "," + b);
        }
        return out;
    }

    public static String value(Headers headers, String name) {
        if (headers == null) {
            return null;
        }
        Header h = headers.lastHeader(name);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private String decode(byte[] value) {
        if (value == null) {
            return "null";
        }
        String s = new String(value, StandardCharsets.UTF_8).replaceAll("\\p{Cntrl}", "?");
        int max = props.maxHeaderValueLength();
        return s.length() > max ? s.substring(0, max) + "...(+" + (s.length() - max) + ")" : s;
    }
}
