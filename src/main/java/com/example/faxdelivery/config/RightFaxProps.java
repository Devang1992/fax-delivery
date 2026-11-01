package com.example.faxdelivery.config;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.example.faxdelivery.http.HttpProps;

/**
 * The whole RightFax REST contract is configuration: paths, auth style, cookie, request field names,
 * response JSON pointers (RFC 6901) and the condition values that count as final.
 * Defaults follow the RightFax Web API as described in our notes - VERIFY against your RightFax version.
 *
 * @param allowInsecureHttp only for local stubs; otherwise a non-https base-url fails startup (credentials and
 *                          the rf-session cookie must never travel in clear text)
 */
@ConfigurationProperties("app.rightfax")
public record RightFaxProps(String baseUrl, Boolean allowInsecureHttp, String username, String password,
                            Login login, Attachments attachments, SendJob sendJob, Job job, Documents documents,
                            Conditions conditions, Set<Integer> permanentStatuses, HttpProps http) {

    public RightFaxProps {
        Objects.requireNonNull(baseUrl, "app.rightfax.base-url is required");
        allowInsecureHttp = allowInsecureHttp != null && allowInsecureHttp;
        username = username == null ? "" : username;
        password = password == null ? "" : password;
        login = login == null ? new Login(null, null, null, null, null, null, null, null) : login;
        attachments = attachments == null ? new Attachments(null, null, null) : attachments;
        sendJob = sendJob == null ? new SendJob(null, null, null, null, null, null, null, null) : sendJob;
        job = job == null ? new Job(null, null, null, null) : job;
        documents = documents == null ? new Documents(null, null, null, null, null, null) : documents;
        conditions = conditions == null ? new Conditions(null, null) : conditions;
        permanentStatuses = permanentStatuses == null ? Set.of(400, 404, 409, 422) : Set.copyOf(permanentStatuses);
        http = http == null ? new HttpProps(null, null, null, null, null, null, null) : http;
    }

    /**
     * Call 1. The session comes back as a cookie and is sent on every later call.
     *
     * @param style       BASIC (Authorization header), JSON (credentials in a JSON body) or FORM (url-encoded)
     * @param sessionTtl  how long a session is reused; null = until RightFax answers 401
     * @param refreshBefore re-login this long before sessionTtl ends
     */
    public record Login(String method, String path, LoginStyle style, String usernameField, String passwordField,
                        String cookieName, Duration sessionTtl, Duration refreshBefore) {

        public Login {
            method = d(method, "POST");
            path = d(path, "/Login");
            style = style == null ? LoginStyle.BASIC : style;
            usernameField = d(usernameField, "username");
            passwordField = d(passwordField, "password");
            cookieName = d(cookieName, "rf-session");
            refreshBefore = refreshBefore == null ? Duration.ofMinutes(1) : refreshBefore;
        }
    }

    public enum LoginStyle {
        BASIC, JSON, FORM
    }

    /** Call 2: multipart upload; the response contains a TEMPORARY attachment URL. */
    public record Attachments(String path, String partName, String urlPointer) {

        public Attachments {
            path = d(path, "/Attachments");
            partName = d(partName, "file");
            urlPointer = d(urlPointer, "/0");
        }
    }

    /**
     * Call 3: send job. Body:
     * { recipientsField: [ { recipientNameField, recipientDestinationField } ],
     *   attachmentUrlsField: [ url ], midField?: MID, ...staticFields }
     */
    public record SendJob(String path, String recipientsField, String recipientNameField,
                          String recipientDestinationField, String attachmentUrlsField, String midField,
                          Map<String, Object> staticFields, String jobIdPointer) {

        public SendJob {
            path = d(path, "/SendJobs");
            recipientsField = d(recipientsField, "Recipients");
            recipientNameField = d(recipientNameField, "Name");
            recipientDestinationField = d(recipientDestinationField, "Destination");
            attachmentUrlsField = d(attachmentUrlsField, "AttachmentUrls");
            staticFields = staticFields == null ? Map.of() : Map.copyOf(staticFields);
            jobIdPointer = d(jobIdPointer, "/Id");
        }
    }

    /** Call 4a: job status. {jobId} in the path is expanded. */
    public record Job(String path, String conditionPointer, String statusPointer, String statusTextPointer) {

        public Job {
            path = d(path, "/SendJobs/{jobId}");
            conditionPointer = d(conditionPointer, "/Condition");
            statusPointer = d(statusPointer, "/Status");
            statusTextPointer = d(statusTextPointer, "/StatusText");
        }
    }

    /**
     * Call 4b: documents of a job. itemsPointer selects the array ("" = the response itself is the array);
     * the field names are read from every item.
     */
    public record Documents(String path, String itemsPointer, String idField, String conditionField,
                            String statusField, String statusTextField) {

        public Documents {
            path = d(path, "/Documents?filter=job&jobid={jobId}");
            itemsPointer = itemsPointer == null ? "/Items" : itemsPointer;
            idField = d(idField, "Id");
            conditionField = d(conditionField, "Condition");
            statusField = d(statusField, "Status");
            statusTextField = d(statusTextField, "StatusText");
        }
    }

    /** Condition values (case-insensitive). Anything else = not final yet. */
    public record Conditions(Set<String> success, Set<String> failure) {

        public Conditions {
            success = success == null ? Set.of("Succeeded") : Set.copyOf(success);
            failure = failure == null ? Set.of("Failed", "Canceled", "Cancelled") : Set.copyOf(failure);
        }
    }

    private static String d(String v, String def) {
        return v == null || v.isBlank() ? def : v;
    }
}
