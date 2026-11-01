package com.example.faxdelivery.task;

import java.util.Map;

/**
 * The value of every record on the retry and poll topics. A task is immutable; every step produces a NEW task
 * (one Kafka record) or nothing. Times are epoch milliseconds.
 *
 * @param payload          original source JSON, kept only while SUBMIT (to rebuild the PDF on retry); null after
 * @param firstSeenAt      source record timestamp; start of the submit deadline
 * @param submitDeadlineAt firstSeenAt + submit max-age (stamped once; config changes don't move it)
 * @param submittedAt      when RightFax accepted the job
 * @param pollDeadlineAt   submittedAt + poll max-age (stamped once)
 * @param notBefore        not processed before this time (also sent as a header)
 */
public record FaxTask(int version, TaskStage stage, String mid, Origin origin, Map<String, String> originHeaders,
                      String payload, long firstSeenAt, long submitDeadlineAt, int attempt, int serverErrorAttempts,
                      int unexpectedCount, String jobId, long submittedAt, long pollDeadlineAt, int pollCount,
                      long notBefore, String lastStatus, String lastError) {

    public static final int CURRENT_VERSION = 1;

    public FaxTask {
        originHeaders = originHeaders == null ? Map.of() : Map.copyOf(originHeaders);
    }

    /** First attempt, built from the source record (attempt 0, due now). */
    public static FaxTask newSubmit(String mid, Origin origin, Map<String, String> originHeaders, String payload,
                                    long firstSeenAt, long submitDeadlineAt, long now) {
        return new FaxTask(CURRENT_VERSION, TaskStage.SUBMIT, mid, origin, originHeaders, payload, firstSeenAt,
                submitDeadlineAt, 0, 0, 0, null, 0L, 0L, 0, now, null, null);
    }

    /** SUBMIT failed and will be retried at notBefore. */
    public FaxTask retryAt(long nextNotBefore, boolean serverError, boolean unexpected, String error) {
        return new FaxTask(version, TaskStage.SUBMIT, mid, origin, originHeaders, payload, firstSeenAt,
                submitDeadlineAt, attempt + 1, serverError ? serverErrorAttempts + 1 : serverErrorAttempts,
                unexpected ? unexpectedCount + 1 : unexpectedCount, null, 0L, 0L, 0, nextNotBefore, lastStatus,
                error);
    }

    /** RightFax accepted the job: first poll task. Payload dropped, unexpected counter reset for the poll phase. */
    public FaxTask submitted(String newJobId, long now, long newPollDeadlineAt, long firstPollAt) {
        return new FaxTask(version, TaskStage.POLL_JOB, mid, origin, originHeaders, null, firstSeenAt,
                submitDeadlineAt, attempt, serverErrorAttempts, 0, newJobId, now, newPollDeadlineAt, 0, firstPollAt,
                null, null);
    }

    /** Not final yet (or the check failed transiently): next poll at notBefore. */
    public FaxTask nextPoll(TaskStage nextStage, long nextNotBefore, String status, String error, boolean unexpected) {
        return new FaxTask(version, nextStage, mid, origin, originHeaders, null, firstSeenAt, submitDeadlineAt,
                attempt, serverErrorAttempts, unexpected ? unexpectedCount + 1 : unexpectedCount, jobId, submittedAt,
                pollDeadlineAt, pollCount + 1, nextNotBefore, status != null ? status : lastStatus, error);
    }

    /** Kafka key: MID, falling back to origin coordinates so a task always has a stable key. */
    public String kafkaKey() {
        return mid != null && !mid.isBlank() && !"n/a".equals(mid) ? mid : origin.coordinates();
    }
}
