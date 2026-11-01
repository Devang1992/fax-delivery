package com.example.faxdelivery.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Kafka header names. Routing headers are written on every retry/poll record so a consumer can filter and
 * check the due time WITHOUT parsing the body.
 */
@ConfigurationProperties("app.headers")
public record HeaderProps(String preference, String mid, String notBefore, String stage, String taskVersion) {

    public HeaderProps {
        preference = d(preference, "PREFERENCE");
        mid = d(mid, "MID");
        notBefore = d(notBefore, "X-FAX-NOT-BEFORE");
        stage = d(stage, "X-FAX-STAGE");
        taskVersion = d(taskVersion, "X-FAX-TASK-VERSION");
    }

    private static String d(String v, String def) {
        return v == null || v.isBlank() ? def : v;
    }
}
