package com.example.faxdelivery.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param templateLocation Handlebars text template, compiled once at startup
 * @param fileNamePattern  attachment file name; {mid} is replaced (sanitised)
 */
@ConfigurationProperties("app.document")
public record DocumentProps(String templateLocation, String fileNamePattern, Pdf pdf) {

    public DocumentProps {
        templateLocation = templateLocation == null || templateLocation.isBlank()
                ? "classpath:templates/GPP.txt" : templateLocation;
        fileNamePattern = fileNamePattern == null || fileNamePattern.isBlank() ? "fax-{mid}.pdf" : fileNamePattern;
        pdf = pdf == null ? new Pdf(null, null, null, null, null, null, null, null) : pdf;
    }

    /** Fax-friendly defaults: Letter, 10pt monospace, top margin leaves room for the fax header line. */
    public record Pdf(String fontLocation, Float fontSize, Float lineSpacing, Float marginTop, Float marginBottom,
                      Float marginLeft, Float marginRight, String pageSize) {

        public Pdf {
            fontLocation = fontLocation == null ? "classpath:fonts/Cousine-Regular.ttf" : fontLocation;
            fontSize = fontSize == null ? 10f : fontSize;
            lineSpacing = lineSpacing == null ? 1.2f : lineSpacing;
            marginTop = marginTop == null ? 54f : marginTop;
            marginBottom = marginBottom == null ? 36f : marginBottom;
            marginLeft = marginLeft == null ? 36f : marginLeft;
            marginRight = marginRight == null ? 36f : marginRight;
            pageSize = pageSize == null ? "LETTER" : pageSize;
        }
    }
}
