package com.example.faxdelivery.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.LoggingProps;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The one place outcome lines are written (logger "fax.outcome"), plus counter fax.outcome{outcome,stage}.
 * Plain format:
 * outcome=... mid=... stage=... origin=... at=... attempt=... polls=... jobId=... detail="..." headers={...}
 * The same fields are SLF4J key/values, so structured (ECS/logstash) output gets them as JSON fields.
 */
@Component
public class OutcomeLogger {

    private static final Logger LOG = LoggerFactory.getLogger("fax.outcome");

    private final MeterRegistry meterRegistry;
    private final int maxDetail;

    public OutcomeLogger(MeterRegistry meterRegistry, LoggingProps props) {
        this.meterRegistry = meterRegistry;
        this.maxDetail = props.maxDetailLength();
    }

    public void log(Outcome outcome, LogContext ctx, String detail) {
        log(outcome, ctx, detail, null);
    }

    /** {@code cause} (with stack trace) only for unexpected exceptions; classified failures are one line. */
    public void log(Outcome outcome, LogContext ctx, String detail, Throwable cause) {
        String d = detail == null ? "" : detail.length() > maxDetail ? detail.substring(0, maxDetail) + "..." : detail;
        LoggingEventBuilder event = LOG.atLevel(outcome.level())
                .addKeyValue("outcome", outcome.name())
                .addKeyValue("mid", ctx.mid())
                .addKeyValue("stage", ctx.stage())
                .addKeyValue("origin", ctx.origin())
                .addKeyValue("at", ctx.at())
                .addKeyValue("attempt", ctx.attempt())
                .addKeyValue("pollCount", ctx.pollCount())
                .addKeyValue("jobId", ctx.jobId())
                .addKeyValue("headers", ctx.headers());
        if (cause != null) {
            event = event.setCause(cause);
        }
        event.log("outcome={} mid={} stage={} origin={} at={} attempt={} polls={} jobId={} detail=\"{}\" headers={}",
                outcome, ctx.mid(), ctx.stage(), ctx.origin(), ctx.at(), ctx.attempt(), ctx.pollCount(),
                ctx.jobId(), d, ctx.headers());
        Counter.builder("fax.outcome")
                .tag("outcome", outcome.name())
                .tag("stage", ctx.stage())
                .register(meterRegistry)
                .increment();
    }
}
