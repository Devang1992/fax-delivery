package com.example.faxdelivery.rightfax;

import java.util.List;

import com.example.faxdelivery.message.FaxRequest;

/**
 * Raw RightFax REST calls: no breaker, no retries, no session caching. May throw WebClient exceptions;
 * contract problems in a 2xx response are thrown as PermanentFailureException.
 * Replace this bean to plug in an existing client (e.g. RFInjector for login).
 */
public interface RightFaxApi {

    /** Returns the session cookie value. */
    String login();

    /** Returns the TEMPORARY attachment URL - use it immediately. */
    String uploadAttachment(String sessionCookie, byte[] pdf, String fileName);

    /** Returns the job id. */
    String sendJob(String sessionCookie, String attachmentUrl, FaxRequest request);

    JobStatus getJob(String sessionCookie, String jobId);

    List<DocumentStatus> getDocuments(String sessionCookie, String jobId);
}
