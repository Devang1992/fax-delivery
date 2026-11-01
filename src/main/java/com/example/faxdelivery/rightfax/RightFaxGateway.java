package com.example.faxdelivery.rightfax;

import java.time.Clock;
import java.util.List;
import java.util.function.Supplier;

import com.example.faxdelivery.config.DocumentProps;
import com.example.faxdelivery.config.RightFaxProps;
import com.example.faxdelivery.error.BreakerRejectedException;
import com.example.faxdelivery.error.FailureException;
import com.example.faxdelivery.error.HttpFailureClassifier;
import com.example.faxdelivery.error.SessionExpiredException;
import com.example.faxdelivery.error.SystemicFailureException;
import com.example.faxdelivery.message.FaxRequest;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;

/**
 * Every RightFax call goes through here: session cookie attached, ONE shared breaker, failures classified.
 * <ul>
 *   <li>Upload and send job run back to back in one method: the temporary attachment URL never leaves memory.</li>
 *   <li>A 401 on a session call becomes SessionExpiredException (re-login + one repeat, ignored by the breaker);
 *       a 401 on login itself stays Systemic (bad credentials, counted by the breaker).</li>
 *   <li>CallNotPermittedException becomes BreakerRejectedException: the record is held, not failed.</li>
 * </ul>
 */
public class RightFaxGateway implements RightFaxOperations {

    private final RightFaxApi api;
    private final CircuitBreaker breaker;
    private final HttpFailureClassifier classifier;
    private final DocumentProps documentProps;
    private final RightFaxSessionManager sessions;

    public RightFaxGateway(RightFaxApi api, CircuitBreaker breaker, HttpFailureClassifier classifier,
                           RightFaxProps props, DocumentProps documentProps, Clock clock) {
        this.api = api;
        this.breaker = breaker;
        this.classifier = classifier;
        this.documentProps = documentProps;
        this.sessions = new RightFaxSessionManager(() -> guarded("RIGHTFAX_LOGIN", api::login, false),
                props.login().sessionTtl(), props.login().refreshBefore(), clock);
    }

    @Override
    public String submit(byte[] pdf, FaxRequest request) {
        String fileName = documentProps.fileNamePattern().replace("{mid}", sanitize(request.mid()));
        String attachmentUrl = sessions.withSession(cookie ->
                guarded("RIGHTFAX_UPLOAD", () -> api.uploadAttachment(cookie, pdf, fileName), true));
        return sessions.withSession(cookie ->
                guarded("RIGHTFAX_SEND_JOB", () -> api.sendJob(cookie, attachmentUrl, request), true));
    }

    @Override
    public JobStatus jobStatus(String jobId) {
        return sessions.withSession(cookie -> guarded("RIGHTFAX_JOB_STATUS", () -> api.getJob(cookie, jobId), true));
    }

    @Override
    public List<DocumentStatus> documents(String jobId) {
        return sessions.withSession(cookie ->
                guarded("RIGHTFAX_DOCUMENTS", () -> api.getDocuments(cookie, jobId), true));
    }

    private <T> T guarded(String operation, Supplier<T> call, boolean sessionCall) {
        try {
            return breaker.executeSupplier(() -> {
                try {
                    return call.get();
                }
                catch (RuntimeException e) {
                    FailureException f = classifier.classify(e, operation);
                    if (sessionCall && f instanceof SystemicFailureException
                            && !(f instanceof SessionExpiredException) && Integer.valueOf(401).equals(f.httpStatus())) {
                        throw new SessionExpiredException(operation, e);
                    }
                    throw f;
                }
            });
        }
        catch (CallNotPermittedException e) {
            throw new BreakerRejectedException(operation, e);
        }
    }

    private static String sanitize(String mid) {
        return mid == null ? "unknown" : mid.replaceAll("[^A-Za-z0-9_-]", "_");
    }
}
