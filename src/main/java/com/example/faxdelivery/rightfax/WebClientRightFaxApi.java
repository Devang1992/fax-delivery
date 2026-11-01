package com.example.faxdelivery.rightfax;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseCookie;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import com.example.faxdelivery.config.RightFaxProps;
import com.example.faxdelivery.error.PermanentFailureException;
import com.example.faxdelivery.error.SystemicFailureException;
import com.example.faxdelivery.message.FaxRequest;

import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * RightFax REST over WebClient. Blocking on purpose: callers run on Kafka listener threads and must know the
 * outcome before the offset is committed. Every call is capped by {@code http.call-timeout}.
 */
public class WebClientRightFaxApi implements RightFaxApi {

    private final WebClient webClient;
    private final RightFaxProps props;
    private final JsonMapper jsonMapper;
    private final RightFaxResponseReader reader;
    private final Duration callTimeout;

    public WebClientRightFaxApi(WebClient webClient, RightFaxProps props, JsonMapper jsonMapper) {
        this.webClient = webClient;
        this.props = props;
        this.jsonMapper = jsonMapper;
        this.reader = new RightFaxResponseReader(jsonMapper);
        this.callTimeout = props.http().callTimeout();
    }

    // ---------------------------------------------------------------- call 1: login
    @Override
    public String login() {
        RightFaxProps.Login l = props.login();
        WebClient.RequestBodySpec spec = webClient.method(HttpMethod.valueOf(l.method().toUpperCase(Locale.ROOT)))
                .uri(l.path());
        WebClient.RequestHeadersSpec<?> request = switch (l.style()) {
            case BASIC -> spec.headers(h -> h.setBasicAuth(props.username(), props.password()));
            case JSON -> spec.contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(jsonMapper.writeValueAsString(
                            Map.of(l.usernameField(), props.username(), l.passwordField(), props.password())));
            case FORM -> spec.contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body(BodyInserters.fromFormData(l.usernameField(), props.username())
                            .with(l.passwordField(), props.password()));
        };
        String cookie = request.exchangeToMono(response -> {
                    if (response.statusCode().is2xxSuccessful()) {
                        ResponseCookie c = response.cookies().getFirst(l.cookieName());
                        String value = c == null ? null : c.getValue();
                        return response.releaseBody().then(Mono.justOrEmpty(value));
                    }
                    return response.<String>createError();
                })
                .timeout(callTimeout)
                .block();
        if (!StringUtils.hasText(cookie)) {
            throw new SystemicFailureException("LOGIN_NO_SESSION_COOKIE",
                    "RIGHTFAX_LOGIN: 2xx without a '" + l.cookieName() + "' cookie", null, null);
        }
        return cookie;
    }

    // ---------------------------------------------------------------- call 2: upload attachment
    @Override
    public String uploadAttachment(String sessionCookie, byte[] pdf, String fileName) {
        RightFaxProps.Attachments a = props.attachments();
        MultipartBodyBuilder parts = new MultipartBodyBuilder();
        parts.part(a.partName(), new ByteArrayResource(pdf) {
            @Override
            public String getFilename() {
                return fileName;
            }
        }).contentType(MediaType.APPLICATION_PDF);
        String body = webClient.post()
                .uri(a.path())
                .cookie(props.login().cookieName(), sessionCookie)
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(parts.build()))
                .retrieve()
                .bodyToMono(String.class)
                .timeout(callTimeout)
                .block();
        String url = reader.text(body, reader.parse(body), a.urlPointer());
        if (!StringUtils.hasText(url)) {
            throw new PermanentFailureException("ATTACHMENT_URL_MISSING",
                    "RIGHTFAX_UPLOAD: 2xx without a value at '" + a.urlPointer() + "' body=" + snippet(body));
        }
        return url;
    }

    // ---------------------------------------------------------------- call 3: send job
    @Override
    public String sendJob(String sessionCookie, String attachmentUrl, FaxRequest request) {
        RightFaxProps.SendJob s = props.sendJob();
        Map<String, Object> recipient = new LinkedHashMap<>();
        recipient.put(s.recipientNameField(), request.recipientName() == null ? "" : request.recipientName());
        recipient.put(s.recipientDestinationField(), request.faxNumber());
        Map<String, Object> job = new LinkedHashMap<>(s.staticFields());
        job.put(s.recipientsField(), List.of(recipient));
        job.put(s.attachmentUrlsField(), List.of(attachmentUrl));
        if (StringUtils.hasText(s.midField())) {
            job.put(s.midField(), request.mid());
        }
        String body = webClient.post()
                .uri(s.path())
                .cookie(props.login().cookieName(), sessionCookie)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonMapper.writeValueAsString(job))
                .retrieve()
                .bodyToMono(String.class)
                .timeout(callTimeout)
                .block();
        String jobId = reader.text(body, reader.parse(body), s.jobIdPointer());
        if (!StringUtils.hasText(jobId)) {
            // The job is very likely queued already - never retry this (a retry would send the fax twice).
            throw new PermanentFailureException("JOB_ID_MISSING",
                    "RIGHTFAX_SEND_JOB: 2xx without a job id at '" + s.jobIdPointer() + "' body=" + snippet(body));
        }
        return jobId;
    }

    // ---------------------------------------------------------------- call 4a: job status
    @Override
    public JobStatus getJob(String sessionCookie, String jobId) {
        RightFaxProps.Job j = props.job();
        String body = webClient.get()
                .uri(j.path(), jobId)
                .cookie(props.login().cookieName(), sessionCookie)
                .retrieve()
                .bodyToMono(String.class)
                .timeout(callTimeout)
                .block();
        JsonNode root = reader.parse(body);
        return new JobStatus(reader.text(body, root, j.conditionPointer()), reader.text(body, root, j.statusPointer()),
                reader.text(body, root, j.statusTextPointer()));
    }

    // ---------------------------------------------------------------- call 4b: documents
    @Override
    public List<DocumentStatus> getDocuments(String sessionCookie, String jobId) {
        RightFaxProps.Documents d = props.documents();
        String body = webClient.get()
                .uri(d.path(), jobId)
                .cookie(props.login().cookieName(), sessionCookie)
                .retrieve()
                .bodyToMono(String.class)
                .timeout(callTimeout)
                .block();
        return reader.items(reader.parse(body), d.itemsPointer()).stream()
                .map(item -> new DocumentStatus(
                        RightFaxResponseReader.field(item, d.idField()),
                        RightFaxResponseReader.field(item, d.conditionField()),
                        RightFaxResponseReader.field(item, d.statusField()),
                        RightFaxResponseReader.field(item, d.statusTextField())))
                .toList();
    }

    private static String snippet(String body) {
        if (body == null) {
            return "";
        }
        return body.length() > 500 ? body.substring(0, 500) + "..." : body;
    }
}
