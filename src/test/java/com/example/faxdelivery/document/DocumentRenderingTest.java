package com.example.faxdelivery.document;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;

import com.example.faxdelivery.config.DocumentProps;
import com.example.faxdelivery.config.MessageProps;
import com.example.faxdelivery.error.PermanentFailureException;
import com.example.faxdelivery.message.FaxRequest;
import com.example.faxdelivery.message.FaxRequestMapper;

import tools.jackson.databind.json.JsonMapper;

class DocumentRenderingTest {

    private final DefaultResourceLoader loader = new DefaultResourceLoader();
    private final DocumentProps.Pdf pdfProps = new DocumentProps.Pdf(null, null, null, null, null, null, null, null);
    private final FaxRequestMapper mapper = new FaxRequestMapper(JsonMapper.builder().build(),
            new MessageProps(null, null, List.of(), null));

    @Test
    void helpers() {
        assertThat(TemplateHelpers.padRight("ab", 4)).isEqualTo("ab  ");
        assertThat(TemplateHelpers.padRight("abcdef", 4)).isEqualTo("abcd");
        assertThat(TemplateHelpers.padLeft("12.50", 8)).isEqualTo("   12.50");
        assertThatThrownBy(() -> TemplateHelpers.padLeft("123456789", 8)).isInstanceOf(IllegalArgumentException.class);
        assertThat(TemplateHelpers.wrap("one two three four", 9)).isEqualTo("one two\nthree\nfour");
    }

    @Test
    void templateIsPlainTextAndUsesThePayload() {
        FaxRequest request = mapper.map("{\"faxNumber\":\"+1 416 555 0100\",\"recipientName\":\"A & B Ltd\","
                + "\"data\":{\"amount\":\"1,000.00\",\"message\":\"x\"}}", "MID-1");
        String text = new FaxTemplateRenderer(loader, "classpath:templates/GPP.txt").render(request.templateContext());
        assertThat(text).contains("A & B Ltd").contains("MID-1").contains("1,000.00").doesNotContain("&amp;");
        assertThat(request.faxNumber()).isEqualTo("+14165550100");
    }

    @Test
    void pdfSurvivesTabsMissingGlyphsAndLongLines() throws Exception {
        byte[] pdf = new TextPdfRenderer(loader, pdfProps)
                .render("col1\tcol2\nemoji \uD83D\uDE00 \u0001ctrl\n" + "X".repeat(500));
        assertThat(new String(pdf, 0, 5)).isEqualTo("%PDF-");
        try (PDDocument doc = Loader.loadPDF(pdf)) {
            assertThat(doc.getNumberOfPages()).isEqualTo(1);
            assertThat(new PDFTextStripper().getText(doc)).contains("col1").contains("col2");
        }
    }

    @Test
    void longDocumentsPaginate() throws Exception {
        byte[] pdf = new TextPdfRenderer(loader, pdfProps).render("line\n".repeat(200));
        try (PDDocument doc = Loader.loadPDF(pdf)) {
            assertThat(doc.getNumberOfPages()).isGreaterThan(1);
        }
    }

    @Test
    void amountWiderThanItsFieldIsPermanent() {
        FaxDocumentFactory factory = new TemplatePdfDocumentFactory(
                new FaxTemplateRenderer(loader, "classpath:templates/GPP.txt"), new TextPdfRenderer(loader, pdfProps));
        FaxRequest request = new FaxRequest("MID-1", "+1", "X", Map.of("data", Map.of("amount", "1".repeat(25))));
        assertThatThrownBy(() -> factory.create(request)).isInstanceOf(PermanentFailureException.class);
    }
}
