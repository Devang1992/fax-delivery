package com.example.faxdelivery.flow;

import org.springframework.stereotype.Component;

import com.example.faxdelivery.config.PollProps;
import com.example.faxdelivery.config.SubmitProps;
import com.example.faxdelivery.document.FaxDocumentFactory;
import com.example.faxdelivery.error.BreakerRejectedException;
import com.example.faxdelivery.error.FailureException;
import com.example.faxdelivery.error.PermanentFailureException;
import com.example.faxdelivery.error.ServerErrorFailureException;
import com.example.faxdelivery.error.Throwables;
import com.example.faxdelivery.logging.Outcome;
import com.example.faxdelivery.message.FaxRequest;
import com.example.faxdelivery.message.FaxRequestMapper;
import com.example.faxdelivery.rightfax.RightFaxOperations;
import com.example.faxdelivery.task.FaxTask;

/**
 * SUBMIT step (first attempt from the source listener, every retry from the retry listener):
 * <ol>
 *   <li>Deadline passed (firstSeenAt + submit max-age)? -> SUBMIT_EXPIRED, nothing is sent.</li>
 *   <li>Map + render + PDF. Bad data -> INVALID_MESSAGE / FAILED_PERMANENT (before any RightFax call, so bad
 *       data never reaches RightFax and never opens the breaker).</li>
 *   <li>login (if needed) + upload + send job, back to back.</li>
 *   <li>Outcome:
 *     <ul>
 *       <li>job id -> first POLL task (poll deadline stamped now)</li>
 *       <li>breaker rejected -> HOLD (never requeued)</li>
 *       <li>201 without job id -> SUBMISSION_UNCONFIRMED (never retried: would duplicate the fax)</li>
 *       <li>other permanent -> FAILED_PERMANENT</li>
 *       <li>500 with cap reached -> FAILED_EXHAUSTED</li>
 *       <li>transient / systemic -> RETRY task at now + retry delay (ambiguous ones logged POSSIBLE_DUPLICATE),
 *           unless that is past the deadline -> SUBMIT_EXPIRED</li>
 *       <li>unclassified exception -> same as transient, at most max-unexpected times -> UNEXPECTED_EXHAUSTED</li>
 *     </ul>
 *   </li>
 * </ol>
 */
@Component
public class SubmitFlow {

    private final FaxRequestMapper mapper;
    private final FaxDocumentFactory documents;
    private final RightFaxOperations rightFax;
    private final SubmitProps submit;
    private final PollProps poll;

    public SubmitFlow(FaxRequestMapper mapper, FaxDocumentFactory documents, RightFaxOperations rightFax,
                      SubmitProps submit, PollProps poll) {
        this.mapper = mapper;
        this.documents = documents;
        this.rightFax = rightFax;
        this.submit = submit;
        this.poll = poll;
    }

    public Decision run(FaxTask task, long now) {
        if (now >= task.submitDeadlineAt()) {
            return new Decision.Terminal(Outcome.SUBMIT_EXPIRED, "submit deadline passed before attempt "
                    + (task.attempt() + 1) + " lastError=" + task.lastError());
        }

        FaxRequest request;
        byte[] pdf;
        try {
            request = mapper.map(task.payload(), task.mid());
        }
        catch (PermanentFailureException e) {
            return new Decision.Terminal(Outcome.INVALID_MESSAGE, e.describe());
        }
        try {
            pdf = documents.create(request);
        }
        catch (PermanentFailureException e) {
            return new Decision.Terminal(Outcome.FAILED_PERMANENT, e.describe());
        }

        try {
            String jobId = rightFax.submit(pdf, request);
            FaxTask pollTask = task.submitted(jobId, now, now + poll.maxAge().toMillis(),
                    now + poll.delay().toMillis());
            return new Decision.PublishPoll(pollTask, Outcome.SUBMITTED,
                    "jobId=" + jobId + " submitAttempt=" + (task.attempt() + 1) + " pdfBytes=" + pdf.length);
        }
        catch (BreakerRejectedException e) {
            return new Decision.Hold(e.getMessage());
        }
        catch (PermanentFailureException e) {
            Outcome outcome = "JOB_ID_MISSING".equals(e.reason()) ? Outcome.SUBMISSION_UNCONFIRMED
                    : Outcome.FAILED_PERMANENT;
            return new Decision.Terminal(outcome, e.describe());
        }
        catch (ServerErrorFailureException e) {
            int max = submit.maxRetriesOnServerError();
            if (max >= 0 && task.serverErrorAttempts() >= max) {
                return new Decision.Terminal(Outcome.FAILED_EXHAUSTED,
                        "serverErrorRetries=" + task.serverErrorAttempts() + " " + e.describe());
            }
            return retry(task, now, e.describe(), e.isAmbiguous(), true, false);
        }
        catch (FailureException e) {
            return retry(task, now, e.describe(), e.isAmbiguous(), false, false);
        }
        catch (RuntimeException e) {
            if (task.unexpectedCount() + 1 > submit.maxUnexpected()) {
                return new Decision.Terminal(Outcome.UNEXPECTED_EXHAUSTED,
                        "unexpected failures=" + (task.unexpectedCount() + 1) + " " + Throwables.rootMessage(e), e);
            }
            return retry(task, now, "reason=UNEXPECTED " + Throwables.rootMessage(e), true, false, true);
        }
    }

    private Decision retry(FaxTask task, long now, String error, boolean ambiguous, boolean serverError,
                           boolean unexpected) {
        long next = now + submit.retryDelay().toMillis();
        if (next >= task.submitDeadlineAt()) {
            return new Decision.Terminal(Outcome.SUBMIT_EXPIRED,
                    "next retry would be past the submit deadline; attempts=" + (task.attempt() + 1)
                            + " lastError: " + error);
        }
        FaxTask retryTask = task.retryAt(next, serverError, unexpected, error);
        return new Decision.PublishRetry(retryTask, ambiguous ? Outcome.POSSIBLE_DUPLICATE : Outcome.RETRY_SCHEDULED,
                "attempt=" + retryTask.attempt() + " retryInMs=" + submit.retryDelay().toMillis() + " " + error);
    }
}
