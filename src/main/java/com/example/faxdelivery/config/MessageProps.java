package com.example.faxdelivery.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Where to find things in the SOURCE message (JSON pointers, RFC 6901).
 *
 * @param faxNumberPointer      e.g. /faxNumber or /recipient/fax
 * @param recipientNamePointer  e.g. /recipientName (optional field)
 * @param requiredPointers      fields that must be present and non-blank, else INVALID_MESSAGE
 * @param faxNumberPattern      validation regex applied to the raw fax number
 */
@ConfigurationProperties("app.message")
public record MessageProps(String faxNumberPointer, String recipientNamePointer, List<String> requiredPointers,
                           String faxNumberPattern) {

    public MessageProps {
        faxNumberPointer = d(faxNumberPointer, "/faxNumber");
        recipientNamePointer = d(recipientNamePointer, "/recipientName");
        requiredPointers = requiredPointers == null ? List.of() : List.copyOf(requiredPointers);
        faxNumberPattern = d(faxNumberPattern, "^\\+?[0-9()\\-.\\s]{7,25}$");
    }

    private static String d(String v, String def) {
        return v == null || v.isBlank() ? def : v;
    }
}
