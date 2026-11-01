package com.example.faxdelivery.rightfax;

import java.util.ArrayList;
import java.util.List;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Reads values out of RightFax responses with JSON pointers. Pure, no I/O. */
public class RightFaxResponseReader {

    private final JsonMapper jsonMapper;

    public RightFaxResponseReader(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    /** Parsed body, or null when the body is empty or not JSON. */
    public JsonNode parse(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            return jsonMapper.readTree(body);
        }
        catch (JacksonException e) {
            return null;
        }
    }

    /** Text at the pointer; "" or "/" means the whole body (some endpoints answer with a bare string). */
    public String text(String body, JsonNode root, String pointer) {
        if (pointer == null || pointer.isEmpty() || "/".equals(pointer)) {
            if (root == null) {
                return body == null ? null : stripQuotes(body.trim());
            }
            return root.isValueNode() ? root.asString() : root.toString();
        }
        if (root == null) {
            return null;
        }
        return valueOf(root.at(pointer));
    }

    /** Elements of the array at the pointer ("" = the root itself); empty when absent or not an array. */
    public List<JsonNode> items(JsonNode root, String pointer) {
        List<JsonNode> out = new ArrayList<>();
        if (root == null) {
            return out;
        }
        JsonNode array = pointer == null || pointer.isEmpty() ? root : root.at(pointer);
        if (array != null && array.isArray()) {
            for (int i = 0; i < array.size(); i++) {
                out.add(array.get(i));
            }
        }
        return out;
    }

    public static String field(JsonNode item, String name) {
        return item == null ? null : valueOf(item.get(name));
    }

    private static String valueOf(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        return node.isValueNode() ? node.asString() : node.toString();
    }

    private static String stripQuotes(String s) {
        return s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"") ? s.substring(1, s.length() - 1) : s;
    }
}
