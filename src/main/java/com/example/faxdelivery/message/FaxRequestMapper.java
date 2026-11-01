package com.example.faxdelivery.message;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.MessageProps;
import com.example.faxdelivery.error.PermanentFailureException;

import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Source payload -> FaxRequest. Field locations are JSON pointers from configuration, so a schema change is a
 * config change. Every failure here is about the data, so it is permanent (logged INVALID_MESSAGE).
 */
@Component
public class FaxRequestMapper {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final JsonMapper jsonMapper;
    private final MessageProps props;
    private final Pattern faxNumberPattern;

    public FaxRequestMapper(JsonMapper jsonMapper, MessageProps props) {
        this.jsonMapper = jsonMapper;
        this.props = props;
        this.faxNumberPattern = Pattern.compile(props.faxNumberPattern());
    }

    public FaxRequest map(String payload, String mid) {
        if (payload == null || payload.isBlank()) {
            throw new PermanentFailureException("EMPTY_PAYLOAD", "record value is null or blank");
        }
        JsonNode root;
        try {
            root = jsonMapper.readTree(payload);
        }
        catch (JacksonException e) {
            throw new PermanentFailureException("INVALID_JSON", e.getOriginalMessage(), null, e);
        }
        if (root == null || !root.isObject()) {
            throw new PermanentFailureException("INVALID_JSON", "payload must be a JSON object");
        }
        String rawFax = text(root, props.faxNumberPointer());
        if (rawFax == null || !faxNumberPattern.matcher(rawFax).matches()) {
            throw new PermanentFailureException("INVALID_FAX_NUMBER",
                    "fax number missing or malformed at " + props.faxNumberPointer());
        }
        List<String> missing = props.requiredPointers().stream()
                .filter(p -> {
                    String v = text(root, p);
                    return v == null || v.isBlank();
                })
                .toList();
        if (!missing.isEmpty()) {
            throw new PermanentFailureException("MISSING_FIELD", "required fields missing: " + missing);
        }
        String faxNumber = rawFax.replaceAll("[^0-9+]", "");
        String recipientName = text(root, props.recipientNamePointer());

        Map<String, Object> context = new LinkedHashMap<>(jsonMapper.convertValue(root, MAP_TYPE));
        context.put("mid", mid);
        context.put("faxNumber", faxNumber);
        context.put("recipientName", recipientName == null ? "" : recipientName);
        return new FaxRequest(mid, faxNumber, recipientName, context);
    }

    private static String text(JsonNode root, String pointer) {
        JsonNode n = root.at(pointer);
        if (n == null || n.isMissingNode() || n.isNull()) {
            return null;
        }
        return n.isValueNode() ? n.asString() : n.toString();
    }
}
