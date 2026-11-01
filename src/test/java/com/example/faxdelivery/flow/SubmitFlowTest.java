package com.example.faxdelivery.flow;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.ConnectException;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.example.faxdelivery.config.MessageProps;
import com.example.faxdelivery.config.PollProps;
import com.example.faxdelivery.config.SubmitProps;
import com.example.faxdelivery.error.BreakerRejectedException;
import com.example.faxdelivery.error.PermanentFailureException;
import com.example.faxdelivery.error.ServerErrorFailureException;
import com.example.faxdelivery.error.TransientFailureException;
import com.example.faxdelivery.logging.Outcome;
import com.example.faxdelivery.message.FaxRequestMapper;
import com.example.faxdelivery.task.FaxTask;
import com.example.faxdelivery.task.TaskStage;

import tools.jackson.databind.json.JsonMapper;

class SubmitFlowTest {

    private static final long NOW = 1_800_000_000_000L;
    private static final long HOUR = 3_600_000L;

    private final FlowFixtures.FakeRightFax rightFax = new FlowFixtures.FakeRightFax();
    private final SubmitProps submit = new SubmitProps(Duration.ofMinutes(1), Duration.ofHours(4), 3, -1);
    private final PollProps poll = new PollProps(Duration.ofSeconds(30), Duration.ofHours(2), 3);
    private final SubmitFlow flow = new SubmitFlow(
            new FaxRequestMapper(JsonMapper.builder().build(), new MessageProps(null, null, List.of(), null)),
            request -> new byte[] {1, 2, 3}, rightFax, submit, poll);

    @Test
    void acceptedJobBecomesFirstPollTaskWithStampedDeadline() {
        Decision d = flow.run(FlowFixtures.submitTask(NOW, NOW + 4 * HOUR), NOW);

        assertThat(d).isInstanceOf(Decision.PublishPoll.class);
        FaxTask next = ((Decision.PublishPoll) d).task();
        assertThat(next.stage()).isEqualTo(TaskStage.POLL_JOB);
        assertThat(next.jobId()).isEqualTo("job-1");
        assertThat(next.payload()).isNull();
        assertThat(next.notBefore()).isEqualTo(NOW + 30_000);
        assertThat(next.pollDeadlineAt()).isEqualTo(NOW + 2 * HOUR);
        assertThat(((Decision.PublishPoll) d).outcome()).isEqualTo(Outcome.SUBMITTED);
    }

    @Test
    void notSentFailureIsRetriedAfterTheFixedDelay() {
        rightFax.submit = () -> {
            throw new TransientFailureException("REQUEST_NOT_SENT", "refused", null, false, new ConnectException());
        };
        Decision d = flow.run(FlowFixtures.submitTask(NOW, NOW + 4 * HOUR), NOW);

        assertThat(d).isInstanceOf(Decision.PublishRetry.class);
        Decision.PublishRetry r = (Decision.PublishRetry) d;
        assertThat(r.outcome()).isEqualTo(Outcome.RETRY_SCHEDULED);
        assertThat(r.task().notBefore()).isEqualTo(NOW + 60_000);
        assertThat(r.task().attempt()).isEqualTo(1);
        assertThat(r.task().payload()).isNotNull();
    }

    @Test
    void ambiguousFailureIsRetriedAndFlaggedAsPossibleDuplicate() {
        rightFax.submit = () -> {
            throw new TransientFailureException("NO_RESPONSE", "timeout", null, true, null);
        };
        Decision d = flow.run(FlowFixtures.submitTask(NOW, NOW + 4 * HOUR), NOW);
        assertThat(((Decision.PublishRetry) d).outcome()).isEqualTo(Outcome.POSSIBLE_DUPLICATE);
    }

    @Test
    void breakerRejectionHoldsTheRecordInsteadOfRequeueing() {
        rightFax.submit = () -> {
            throw new BreakerRejectedException("RIGHTFAX_UPLOAD", null);
        };
        assertThat(flow.run(FlowFixtures.submitTask(NOW, NOW + 4 * HOUR), NOW)).isInstanceOf(Decision.Hold.class);
    }

    @Test
    void missingJobIdIsNeverRetried() {
        rightFax.submit = () -> {
            throw new PermanentFailureException("JOB_ID_MISSING", "no id");
        };
        Decision d = flow.run(FlowFixtures.submitTask(NOW, NOW + 4 * HOUR), NOW);
        assertThat(((Decision.Terminal) d).outcome()).isEqualTo(Outcome.SUBMISSION_UNCONFIRMED);
    }

    @Test
    void expiredDeadlineSendsNothing() {
        Decision d = flow.run(FlowFixtures.submitTask(NOW - 5 * HOUR, NOW - HOUR), NOW);
        assertThat(((Decision.Terminal) d).outcome()).isEqualTo(Outcome.SUBMIT_EXPIRED);
        assertThat(rightFax.calls).isEmpty();
    }

    @Test
    void retryThatWouldLandPastTheDeadlineExpiresNow() {
        rightFax.submit = () -> {
            throw new TransientFailureException("UNAVAILABLE", "503", 503, false, null);
        };
        Decision d = flow.run(FlowFixtures.submitTask(NOW, NOW + 30_000), NOW);
        assertThat(((Decision.Terminal) d).outcome()).isEqualTo(Outcome.SUBMIT_EXPIRED);
    }

    @Test
    void invalidPayloadNeverReachesRightFax() {
        FaxTask bad = FaxTask.newSubmit("MID-1", FlowFixtures.submitTask(NOW, NOW).origin(), null,
                "{\"faxNumber\":\"abc\"}", NOW, NOW + HOUR, NOW);
        Decision d = flow.run(bad, NOW);
        assertThat(((Decision.Terminal) d).outcome()).isEqualTo(Outcome.INVALID_MESSAGE);
        assertThat(rightFax.calls).isEmpty();
    }

    @Test
    void serverErrorCapIsRespectedWhenConfigured() {
        SubmitFlow capped = new SubmitFlow(
                new FaxRequestMapper(JsonMapper.builder().build(), new MessageProps(null, null, List.of(), null)),
                request -> new byte[] {1}, rightFax, new SubmitProps(Duration.ofMinutes(1), Duration.ofHours(4), 3, 1),
                poll);
        rightFax.submit = () -> {
            throw new ServerErrorFailureException("SERVER_ERROR", "500", 500, null);
        };
        FaxTask first = FlowFixtures.submitTask(NOW, NOW + 4 * HOUR);
        Decision d1 = capped.run(first, NOW);
        assertThat(d1).isInstanceOf(Decision.PublishRetry.class);
        Decision d2 = capped.run(((Decision.PublishRetry) d1).task(), NOW + 60_000);
        assertThat(((Decision.Terminal) d2).outcome()).isEqualTo(Outcome.FAILED_EXHAUSTED);
    }

    @Test
    void unexpectedExceptionsAreRetriedThenGiveUp() {
        rightFax.submit = () -> {
            throw new IllegalStateException("bug");
        };
        FaxTask t = FlowFixtures.submitTask(NOW, NOW + 4 * HOUR);
        long now = NOW;
        for (int i = 0; i < 3; i++) {
            Decision d = flow.run(t, now);
            assertThat(d).isInstanceOf(Decision.PublishRetry.class);
            t = ((Decision.PublishRetry) d).task();
            now += 60_000;
        }
        assertThat(((Decision.Terminal) flow.run(t, now)).outcome()).isEqualTo(Outcome.UNEXPECTED_EXHAUSTED);
    }
}
