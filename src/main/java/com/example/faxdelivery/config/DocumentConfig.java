package com.example.faxdelivery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;

import com.example.faxdelivery.document.FaxDocumentFactory;
import com.example.faxdelivery.document.FaxTemplateRenderer;
import com.example.faxdelivery.document.TemplatePdfDocumentFactory;
import com.example.faxdelivery.document.TextPdfRenderer;

@Configuration(proxyBeanMethods = false)
public class DocumentConfig {

    /** Template compiled and font loaded at startup: a broken template fails the boot, not a fax. */
    @Bean
    FaxDocumentFactory faxDocumentFactory(ResourceLoader resourceLoader, DocumentProps props) {
        return new TemplatePdfDocumentFactory(new FaxTemplateRenderer(resourceLoader, props.templateLocation()),
                new TextPdfRenderer(resourceLoader, props.pdf()));
    }
}
