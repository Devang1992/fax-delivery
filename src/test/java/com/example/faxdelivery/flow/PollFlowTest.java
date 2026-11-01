package com.example.faxdelivery.flow;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.example.faxdelivery.config.PollProps;
import com.example.faxdelivery.error.BreakerRejectedException;
import com.example.faxdelivery.error.PermanentFailureException;
import com.example.faxdelivery.error.TransientFailureException;
import com.example.faxdelivery.logging.Outcome;
import com.example.faxdelivery.rightfax.ConditionResolver;
import com.example.faxdelivery.rightfax.DocumentStatus;
import com.example.faxdelivery.rightfax.JobStatus;
import com.example.faxdelivery.task.FaxTask;
import com.example.faxdelivery.task.TaskStage;

class PollFlowTest {

    private static final long NOW = 1_800_000_000_000L;
    private static final long HOUR = 3_600_000L;

    private final FlowFixtures.FakeRightFax rightFax = new FlowFixtures.FakeRightFax();
    private final PollFlow flow = new PollFlow(rightFax,
            new ConditionResolver(Set.of("Succeeded"), Set.of("Failed", "Canceled")),
            new PollProps(Duration.ofSeconds(30), Duration.ofHours(2), 3));

    @Test
    void pendingJobIsCheckedAgainLater() {
        Decision d = flow.run(FlowFixtures.pollTask(NOW, NOW + 2 * HOUR), NOW + 30_000);
        Decision.PublishPoll p = (Decision.PublishPoll) d;
        assertThat(p.outcome()).isEqualTo(Outcome.STATUS_PENDING);
        assertThat(p.task().stage()).isEqualTo(TaskStage.POLL_JOB);
        assertThat(p.task().pollCount()).isEqualTo(1);
        assertThat(p.task().notBefore()).isEqualTo(NOW + 60_000);
    }

    @Test
    void succeededJobChecksDocumentsInTheSameStep() {
        rightFax.job = () -> new JobStatus("Succeeded", "Done", null);
        rightFax.docs = () -> List.of(new DocumentStatus("d1", "Succeeded", "Sent", null));
        Decision d = flow.run(FlowFixtures.pollTask(NOW, NOW + 2 * HOUR), NOW + 30_000);
        assertThat(((Decision.Terminal) d).outcome()).isEqualTo(Outcome.DELIVERED);
        assertThat(rightFax.calls).containsExactly("job", "docs");
    }

    @Test
    void documentsStillSendingMoveToPollDocs() {
        rightFax.job = () -> new JobStatus("Succeeded", "Done", null);
        rightFax.docs = () -> List.of(new DocumentStatus("d1", "Processing", "Dialing", null));
        Decision.PublishPoll p = (Decision.PublishPoll) flow.run(FlowFixtures.pollTask(NOW, NOW + 2 * HOUR), NOW);
        assertThat(p.task().stage()).isEqualTo(TaskStage.POLL_DOCS);
    }

    @Test
    void failedDocumentIsAFinalFailure() {
        FaxTask docsTask = FlowFixtures.pollTask(NOW, NOW + 2 * HOUR).nextPoll(TaskStage.POLL_DOCS, NOW, null, null,
                false);
        rightFax.docs = () -> List.of(new DocumentStatus("d1", "Failed", "NoAnswer", "busy"));
        Decision.Terminal t = (Decision.Terminal) flow.run(docsTask, NOW);
        assertThat(t.outcome()).isEqualTo(Outcome.FAILED_FINAL_STATUS);
        assertThat(t.detail()).contains("busy");
    }

    @Test
    void transientErrorReschedulesAndNeverKillsTheJobBeforeTheDeadline() {
        rightFax.job = () -> {
            throw new TransientFailureException("UNAVAILABLE", "503", 503, false, null);
        };
        Decision.PublishPoll p = (Decision.PublishPoll) flow.run(FlowFixtures.pollTask(NOW, NOW + 2 * HOUR), NOW);
        assertThat(p.outcome()).isEqualTo(Outcome.STATUS_CHECK_RETRY);
        assertThat(p.task().lastError()).contains("UNAVAILABLE");
    }

    @Test
    void deadlineIsCheckedAfterARealFinalCall() {
        Decision.Terminal t = (Decision.Terminal) flow.run(FlowFixtures.pollTask(NOW, NOW + 2 * HOUR),
                NOW + 2 * HOUR);
        assertThat(t.outcome()).isEqualTo(Outcome.FINAL_TIMEOUT);
        assertThat(rightFax.calls).containsExactly("job");
    }

    @Test
    void timeoutDuringAnOutageIsReportedAsUnreachable() {
        rightFax.job = () -> {
            throw new TransientFailureException("REQUEST_NOT_SENT", "refused", null, false, null);
        };
        Decision.Terminal t = (Decision.Terminal) flow.run(FlowFixtures.pollTask(NOW, NOW + 2 * HOUR),
                NOW + 3 * HOUR);
        assertThat(t.outcome()).isEqualTo(Outcome.FINAL_TIMEOUT_UNREACHABLE);
    }

    @Test
    void unknownJobIsTerminal() {
        rightFax.job = () -> {
            throw new PermanentFailureException("CLIENT_ERROR", "404", 404, null);
        };
        Decision.Terminal t = (Decision.Terminal) flow.run(FlowFixtures.pollTask(NOW, NOW + 2 * HOUR), NOW);
        assertThat(t.outcome()).isEqualTo(Outcome.STATUS_CHECK_FAILED);
    }

    @Test
    void breakerRejectionHolds() {
        rightFax.job = () -> {
            throw new BreakerRejectedException("RIGHTFAX_JOB_STATUS", null);
        };
        assertThat(flow.run(FlowFixtures.pollTask(NOW, NOW + 2 * HOUR), NOW)).isInstanceOf(Decision.Hold.class);
    }
}
