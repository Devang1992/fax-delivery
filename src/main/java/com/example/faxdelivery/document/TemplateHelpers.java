package com.example.faxdelivery.document;

import java.util.ArrayList;
import java.util.List;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Options;

/**
 * Fixed-width helpers for the text template. Register your own (e.g. your wrap helper) in {@link #register}.
 * <pre>
 *   {{padRight data.name 30}}   left-aligned, padded / truncated to 30
 *   {{padLeft data.amount 15}}  right-aligned; REFUSES to truncate (a cut amount is a defect, not formatting)
 *   {{wrap data.message 80}}    word-wrap at 80 columns, long words hard-split
 * </pre>
 */
public final class TemplateHelpers {

    private TemplateHelpers() {
    }

    public static void register(Handlebars handlebars) {
        handlebars.registerHelper("padRight", (Object value, Options options) -> padRight(str(value), width(options)));
        handlebars.registerHelper("padLeft", (Object value, Options options) -> padLeft(str(value), width(options)));
        handlebars.registerHelper("wrap", (Object value, Options options) -> wrap(str(value), width(options)));
    }

    public static String padRight(String value, int width) {
        return value.length() >= width ? value.substring(0, width) : value + " ".repeat(width - value.length());
    }

    public static String padLeft(String value, int width) {
        if (value.length() > width) {
            throw new IllegalArgumentException("value '" + value + "' longer than field width " + width);
        }
        return " ".repeat(width - value.length()) + value;
    }

    public static String wrap(String value, int width) {
        List<String> lines = new ArrayList<>();
        for (String paragraph : value.split("\\r?\\n", -1)) {
            StringBuilder line = new StringBuilder();
            for (String word : paragraph.split(" +")) {
                while (word.length() > width) {
                    if (!line.isEmpty()) {
                        lines.add(line.toString());
                        line.setLength(0);
                    }
                    lines.add(word.substring(0, width));
                    word = word.substring(width);
                }
                if (line.isEmpty()) {
                    line.append(word);
                }
                else if (line.length() + 1 + word.length() <= width) {
                    line.append(' ').append(word);
                }
                else {
                    lines.add(line.toString());
                    line.setLength(0);
                    line.append(word);
                }
            }
            lines.add(line.toString());
        }
        return String.join("\n", lines);
    }

    private static String str(Object value) {
        return value == null ? "" : value.toString();
    }

    private static int width(Options options) {
        Object w = options.param(0, 0);
        int width = w instanceof Number n ? n.intValue() : Integer.parseInt(w.toString());
        if (width <= 0) {
            throw new IllegalArgumentException("helper width must be > 0");
        }
        return width;
    }
}
