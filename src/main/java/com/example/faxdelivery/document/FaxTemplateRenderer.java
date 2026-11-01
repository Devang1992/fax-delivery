package com.example.faxdelivery.document;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import com.example.faxdelivery.error.PermanentFailureException;
import com.github.jknack.handlebars.EscapingStrategy;
import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;

/**
 * Compiles the text template ONCE at startup (missing/broken template = the app does not start).
 * HTML escaping is off: this is plain text, '&' must stay '&'.
 */
public class FaxTemplateRenderer {

    private final Template template;
    private final String location;

    public FaxTemplateRenderer(ResourceLoader resourceLoader, String location) {
        this.location = location;
        Resource resource = resourceLoader.getResource(location);
        try (InputStream in = resource.getInputStream()) {
            Handlebars handlebars = new Handlebars().with(EscapingStrategy.NOOP);
            TemplateHelpers.register(handlebars);
            this.template = handlebars.compileInline(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            throw new IllegalStateException("cannot load fax template " + location, e);
        }
    }

    public String render(Map<String, Object> context) {
        try {
            return template.apply(context);
        }
        catch (IOException | RuntimeException e) {
            throw new PermanentFailureException("TEMPLATE_RENDER", location + ": " + e.getMessage(), null, e);
        }
    }
}
