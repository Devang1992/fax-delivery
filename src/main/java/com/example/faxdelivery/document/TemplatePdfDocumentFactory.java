package com.example.faxdelivery.document;

import com.example.faxdelivery.message.FaxRequest;

/** Handlebars text template -> fixed-width PDF. */
public class TemplatePdfDocumentFactory implements FaxDocumentFactory {

    private final FaxTemplateRenderer templateRenderer;
    private final TextPdfRenderer pdfRenderer;

    public TemplatePdfDocumentFactory(FaxTemplateRenderer templateRenderer, TextPdfRenderer pdfRenderer) {
        this.templateRenderer = templateRenderer;
        this.pdfRenderer = pdfRenderer;
    }

    @Override
    public byte[] create(FaxRequest request) {
        return pdfRenderer.render(templateRenderer.render(request.templateContext()));
    }
}
