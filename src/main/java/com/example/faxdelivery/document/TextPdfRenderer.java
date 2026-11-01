package com.example.faxdelivery.document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType0Font;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import com.example.faxdelivery.config.DocumentProps;
import com.example.faxdelivery.error.PermanentFailureException;

/**
 * Plain text -> PDF (PDFBox 3), one monospace line per text line. Handles PDFBox's failure points up front:
 * showText() rejects \n and \t (split / expanded), Standard-14 fonts only encode WinAnsi (embedded TTF used,
 * missing glyphs become '?'), over-long lines are hard-wrapped (never clipped), pagination by line count.
 */
public class TextPdfRenderer {

    private static final Logger log = LoggerFactory.getLogger(TextPdfRenderer.class);

    private final DocumentProps.Pdf props;
    private final byte[] fontBytes;
    private final PDRectangle pageSize;

    public TextPdfRenderer(ResourceLoader resourceLoader, DocumentProps.Pdf props) {
        this.props = props;
        this.pageSize = "A4".equalsIgnoreCase(props.pageSize()) ? PDRectangle.A4 : PDRectangle.LETTER;
        this.fontBytes = loadFont(resourceLoader, props.fontLocation());
    }

    public byte[] render(String text) {
        try (PDDocument doc = new PDDocument()) {
            PDFont font = fontBytes != null
                    ? PDType0Font.load(doc, new ByteArrayInputStream(fontBytes), true)
                    : new PDType1Font(Standard14Fonts.FontName.COURIER);
            float fontSize = props.fontSize();
            float leading = fontSize * props.lineSpacing();
            float usableWidth = pageSize.getWidth() - props.marginLeft() - props.marginRight();
            float usableHeight = pageSize.getHeight() - props.marginTop() - props.marginBottom();
            float charWidth = font.getStringWidth("M") / 1000f * fontSize;
            int maxChars = Math.max(1, (int) Math.floor(usableWidth / charWidth));
            int linesPerPage = Math.max(1, (int) Math.floor(usableHeight / leading));

            List<String> lines = layout(text, maxChars, font);
            for (int start = 0; start < lines.size(); start += linesPerPage) {
                PDPage page = new PDPage(pageSize);
                doc.addPage(page);
                try (PDPageContentStream cs = new PDPageContentStream(doc, page)) {
                    cs.beginText();
                    cs.setFont(font, fontSize);
                    cs.setLeading(leading);
                    cs.newLineAtOffset(props.marginLeft(), pageSize.getHeight() - props.marginTop() - fontSize);
                    for (String line : lines.subList(start, Math.min(start + linesPerPage, lines.size()))) {
                        cs.showText(line);
                        cs.newLine();
                    }
                    cs.endText();
                }
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            doc.save(out);
            return out.toByteArray();
        }
        catch (IOException | RuntimeException e) {
            throw new PermanentFailureException("PDF_GENERATION", e.toString(), null, e);
        }
    }

    private static List<String> layout(String text, int maxChars, PDFont font) {
        List<String> out = new ArrayList<>();
        String normalized = (text == null ? "" : text).replace("\r\n", "\n").replace('\r', '\n')
                .replace("\t", "    ").replaceAll("[\\p{Cntrl}&&[^\\n]]", "");
        for (String line : normalized.split("\n", -1)) {
            String safe = encodable(line, font);
            if (safe.isEmpty()) {
                out.add("");
            }
            for (int i = 0; i < safe.length(); i += maxChars) {
                out.add(safe.substring(i, Math.min(i + maxChars, safe.length())));
            }
        }
        while (out.size() > 1 && out.get(out.size() - 1).isEmpty()) {
            out.remove(out.size() - 1);
        }
        return out.isEmpty() ? List.of("") : out;
    }

    private static String encodable(String line, PDFont font) {
        try {
            font.encode(line);
            return line;
        }
        catch (IOException | IllegalArgumentException e) {
            StringBuilder sb = new StringBuilder(line.length());
            line.codePoints().forEach(cp -> {
                String ch = new String(Character.toChars(cp));
                try {
                    font.encode(ch);
                    sb.append(ch);
                }
                catch (IOException | IllegalArgumentException ex) {
                    sb.append('?');
                }
            });
            return sb.toString();
        }
    }

    private static byte[] loadFont(ResourceLoader resourceLoader, String location) {
        Resource resource = resourceLoader.getResource(location);
        if (!resource.exists()) {
            log.warn("PDF font {} not found; falling back to Standard-14 Courier (WinAnsi only)", location);
            return null;
        }
        try (InputStream in = resource.getInputStream()) {
            return in.readAllBytes();
        }
        catch (IOException e) {
            throw new IllegalStateException("cannot read PDF font " + location, e);
        }
    }
}
