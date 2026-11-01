package com.example.faxdelivery.task;

public enum TaskStage {
    /** render + PDF + upload + send job (retry topic). */
    SUBMIT,
    /** GET job status (poll topic). */
    POLL_JOB,
    /** job succeeded; GET documents until every document is final (poll topic). */
    POLL_DOCS
}
