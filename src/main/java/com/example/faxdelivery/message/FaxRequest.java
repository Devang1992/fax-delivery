package com.example.faxdelivery.message;

import java.util.Map;

/**
 * Validated fax request built from the source payload.
 *
 * @param faxNumber       normalised (digits and leading +)
 * @param templateContext the whole source payload + mid, faxNumber, recipientName (for the Handlebars template)
 */
public record FaxRequest(String mid, String faxNumber, String recipientName, Map<String, Object> templateContext) {

    public FaxRequest {
        templateContext = templateContext == null ? Map.of() : templateContext;
    }
}
