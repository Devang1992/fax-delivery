package com.example.faxdelivery.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * Fails startup when configuration values contradict each other. Every rule here protects against a real
 * failure mode (rebalance storms, duplicate faxes, credentials in clear text, deadlines that can never be met).
 */
@Component
public class StartupInvariants implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(StartupInvariants.class);
    private static final Duration MARGIN = Duration.ofSeconds(30);

    private final TopicProps topics;
    private final ConsumerProps consumers;
    private final KafkaProps kafka;
    private final SubmitProps submit;
    private final PollProps poll;
    private final TimingProps timing;
    private final RightFaxProps rightFax;
    private final BreakerProps breaker;

    public StartupInvariants(TopicProps topics, ConsumerProps consumers, KafkaProps kafka, SubmitProps submit,
                             PollProps poll, TimingProps timing, RightFaxProps rightFax, BreakerProps breaker) {
        this.topics = topics;
        this.consumers = consumers;
        this.kafka = kafka;
        this.submit = submit;
        this.poll = poll;
        this.timing = timing;
        this.rightFax = rightFax;
        this.breaker = breaker;
    }

    @Override
    public void afterPropertiesSet() {
        List<String> violations = new ArrayList<>();
        Duration call = rightFax.http().callTimeout();
        // login + upload + send, plus one re-login and one repeat after a 401, plus rendering
        Duration submitStep = call.multipliedBy(5).plusSeconds(10);
        // job + documents, plus one re-login and one repeat after a 401
        Duration pollStep = call.multipliedBy(4);

        if (!rightFax.allowInsecureHttp() && !rightFax.baseUrl().toLowerCase().startsWith("https://")) {
            violations.add("app.rightfax.base-url must be https:// (credentials and the session cookie would travel "
                    + "in clear text); set app.rightfax.allow-insecure-http=true only for local stubs");
        }
        if (topics.source().equals(topics.retry()) || topics.source().equals(topics.poll())
                || topics.retry().equals(topics.poll())) {
            violations.add("app.topics.source, retry and poll must be three different topics");
        }
        if (call.compareTo(rightFax.http().connectTimeout().plus(rightFax.http().responseTimeout())) < 0) {
            violations.add("app.rightfax.http.call-timeout must be >= connect-timeout + response-timeout");
        }
        if (submit.maxAge().compareTo(submit.retryDelay().multipliedBy(2)) < 0) {
            violations.add("app.submit.max-age must allow at least two retries (>= 2 x retry-delay)");
        }
        if (poll.maxAge().compareTo(poll.delay().multipliedBy(2)) < 0) {
            violations.add("app.poll.max-age must allow at least two polls (>= 2 x delay)");
        }
        if (kafka.transactionTimeout().compareTo(submitStep.plus(MARGIN)) < 0) {
            violations.add("app.kafka.transaction-timeout must be >= worst submit step (" + submitStep + ") + 30s");
        }
        check(violations, "source", consumers.source(), Duration.ZERO, submitStep);
        check(violations, "retry", consumers.retry(), submit.retryDelay().plus(timing.dueWaitSlack()), submitStep);
        check(violations, "poll", consumers.poll(), poll.delay().plus(timing.dueWaitSlack()), pollStep);
        if (breaker.permittedCallsInHalfOpenState() < 1) {
            violations.add("app.circuit-breaker.permitted-calls-in-half-open-state must be >= 1");
        }
        if (timing.holdBackoff().compareTo(breaker.waitDurationInOpenState()) >= 0) {
            violations.add("app.timing.hold-backoff must be shorter than the breaker's wait-duration-in-open-state");
        }

        if (!violations.isEmpty()) {
            throw new IllegalStateException("Invalid fax-delivery configuration:\n - " + String.join("\n - ",
                    violations));
        }
        log.info("Configuration invariants OK (worst submit step {}, worst poll step {})", submitStep, pollStep);
    }

    /** Between two polls a consumer may wait for the first record and then process max-poll-records records. */
    private static void check(List<String> violations, String name, ConsumerProps.Settings s, Duration wait,
                              Duration step) {
        Duration worst = wait.plus(step.multipliedBy(s.maxPollRecords())).plus(MARGIN);
        if (s.maxPollInterval().compareTo(worst) < 0) {
            violations.add("app.consumers." + name + ".max-poll-interval (" + s.maxPollInterval() + ") must be >= "
                    + worst + " (wait + max-poll-records x worst step + 30s); lower max-poll-records or raise it");
        }
    }
}
