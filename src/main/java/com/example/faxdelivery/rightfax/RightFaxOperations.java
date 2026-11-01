package com.example.faxdelivery.rightfax;

import java.util.List;

import com.example.faxdelivery.message.FaxRequest;

/**
 * RightFax as the flows see it. Every method is breaker-guarded and session-managed, and throws only:
 * the failure taxonomy (Permanent / Transient / ServerError / Systemic) or BreakerRejectedException.
 */
public interface RightFaxOperations {

    /** Calls 1-3 back to back: login (only without a valid session), upload attachment, send job -> job id. */
    String submit(byte[] pdf, FaxRequest request);

    /** Call 4a. */
    JobStatus jobStatus(String jobId);

    /** Call 4b. */
    List<DocumentStatus> documents(String jobId);
}
