package com.example.faxdelivery.logging;

import org.slf4j.MDC;

/** MID / origin / stage on every log line written while a record is handled (incl. WebClient, Spring Kafka). */
public final class MdcScope implements AutoCloseable {

    private static final String[] KEYS = {"mid", "origin", "at", "stage"};

    private MdcScope() {
    }

    public static MdcScope open(LogContext ctx) {
        MDC.put("mid", ctx.mid());
        MDC.put("origin", ctx.origin());
        MDC.put("at", ctx.at());
        MDC.put("stage", ctx.stage());
        return new MdcScope();
    }

    @Override
    public void close() {
        for (String key : KEYS) {
            MDC.remove(key);
        }
    }
}
