package com.example.faxdelivery.flow;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.PollProps;
import com.example.faxdelivery.error.BreakerRejectedException;
import com.example.faxdelivery.error.FailureException;
import com.example.faxdelivery.error.PermanentFailureException;
import com.example.faxdelivery.error.Throwables;
import com.example.faxdelivery.logging.Outcome;
import com.example.faxdelivery.rightfax.Condition;
import com.example.faxdelivery.rightfax.ConditionResolver;
import com.example.faxdelivery.rightfax.DocumentStatus;
import com.example.faxdelivery.rightfax.JobStatus;
import com.example.faxdelivery.rightfax.RightFaxOperations;
import com.example.faxdelivery.task.FaxTask;
import com.example.faxdelivery.task.TaskStage;

/**
 * POLL step. One record = ONE check of ONE job; the listener never waits on a job.
 * <pre>
 * POLL_JOB  : GET job  -> Succeeded: GET documents now (same step) -> see POLL_DOCS
 *                      -> Failed/Canceled: FAILED_FINAL_STATUS
 *                      -> other: not final
 * POLL_DOCS : GET docs -> all Succeeded: DELIVERED; any Failed/Canceled: FAILED_FINAL_STATUS; else not final
 * not final (or transient error / unclassified): now >= pollDeadlineAt ? FINAL_TIMEOUT(_UNREACHABLE)
 *                                                                   : next poll task at now + poll delay
 * 4xx (e.g. 404 job unknown): STATUS_CHECK_FAILED.   Breaker rejected: HOLD.
 * </pre>
 * The deadline is checked AFTER the call, so the last check before giving up is always a real one.
 */
@Component
public class PollFlow {

    private final RightFaxOperations rightFax;
    private final ConditionResolver conditions;
    private final PollProps poll;

    public PollFlow(RightFaxOperations rightFax, ConditionResolver conditions, PollProps poll) {
        this.rightFax = rightFax;
        this.conditions = conditions;
        this.poll = poll;
    }

    public Decision run(FaxTask task, long now) {
        try {
            if (task.stage() == TaskStage.POLL_JOB) {
                JobStatus job = rightFax.jobStatus(task.jobId());
                return switch (conditions.resolve(job.condition())) {
                    case FAILURE -> new Decision.Terminal(Outcome.FAILED_FINAL_STATUS,
                            "jobId=" + task.jobId() + " " + job.describe());
                    case SUCCESS -> evaluateDocuments(task, now, rightFax.documents(task.jobId()));
                    case PENDING -> notFinal(task, now, TaskStage.POLL_JOB, job.describe(), null, false);
                };
            }
            return evaluateDocuments(task, now, rightFax.documents(task.jobId()));
        }
        catch (BreakerRejectedException e) {
            return new Decision.Hold(e.getMessage());
        }
        catch (PermanentFailureException e) {
            return new Decision.Terminal(Outcome.STATUS_CHECK_FAILED, "jobId=" + task.jobId() + " " + e.describe());
        }
        catch (FailureException e) {
            return notFinal(task, now, task.stage(), null, e.describe(), false);
        }
        catch (RuntimeException e) {
            if (task.unexpectedCount() + 1 > poll.maxUnexpected()) {
                return new Decision.Terminal(Outcome.UNEXPECTED_EXHAUSTED, "jobId=" + task.jobId()
                        + " unexpected failures=" + (task.unexpectedCount() + 1) + " " + Throwables.rootMessage(e), e);
            }
            return notFinal(task, now, task.stage(), null, "reason=UNEXPECTED " + Throwables.rootMessage(e), true);
        }
    }

    private Decision evaluateDocuments(FaxTask task, long now, List<DocumentStatus> docs) {
        if (docs.isEmpty()) {
            return notFinal(task, now, TaskStage.POLL_DOCS, "documents=0", null, false);
        }
        List<DocumentStatus> failed = docs.stream()
                .filter(d -> conditions.resolve(d.condition()) == Condition.FAILURE)
                .toList();
        if (!failed.isEmpty()) {
            return new Decision.Terminal(Outcome.FAILED_FINAL_STATUS, "jobId=" + task.jobId() + " failedDocuments="
                    + failed.size() + "/" + docs.size() + " "
                    + failed.stream().map(DocumentStatus::describe).collect(Collectors.joining("; ")));
        }
        boolean allSucceeded = docs.stream().allMatch(d -> conditions.resolve(d.condition()) == Condition.SUCCESS);
        if (allSucceeded) {
            return new Decision.Terminal(Outcome.DELIVERED, "jobId=" + task.jobId() + " documents=" + docs.size()
                    + " polls=" + (task.pollCount() + 1) + " secondsSinceSubmit=" + (now - task.submittedAt()) / 1000);
        }
        String pending = docs.stream().filter(d -> conditions.resolve(d.condition()) == Condition.PENDING)
                .map(DocumentStatus::describe).collect(Collectors.joining("; "));
        return notFinal(task, now, TaskStage.POLL_DOCS, pending, null, false);
    }

    private Decision notFinal(FaxTask task, long now, TaskStage stage, String status, String error,
                              boolean unexpected) {
        if (now >= task.pollDeadlineAt()) {
            Outcome outcome = error != null ? Outcome.FINAL_TIMEOUT_UNREACHABLE : Outcome.FINAL_TIMEOUT;
            return new Decision.Terminal(outcome, "jobId=" + task.jobId() + " stage=" + stage + " polls="
                    + (task.pollCount() + 1) + " maxAge=" + poll.maxAge() + " lastStatus="
                    + (status != null ? status : task.lastStatus()) + (error != null ? " lastError: " + error : ""));
        }
        FaxTask next = task.nextPoll(stage, now + poll.delay().toMillis(), status, error, unexpected);
        return new Decision.PublishPoll(next, error != null ? Outcome.STATUS_CHECK_RETRY : Outcome.STATUS_PENDING,
                "jobId=" + task.jobId() + " stage=" + stage + " nextPollInMs=" + poll.delay().toMillis()
                        + (status != null ? " status: " + status : "") + (error != null ? " error: " + error : ""));
    }
}
