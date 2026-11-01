package com.example.faxdelivery.flow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.example.faxdelivery.message.FaxRequest;
import com.example.faxdelivery.rightfax.DocumentStatus;
import com.example.faxdelivery.rightfax.JobStatus;
import com.example.faxdelivery.rightfax.RightFaxOperations;
import com.example.faxdelivery.task.FaxTask;
import com.example.faxdelivery.task.Origin;

/** Test doubles and task builders shared by the flow tests. */
final class FlowFixtures {

    static final String PAYLOAD = """
            {"faxNumber":"+1 416 555 0100","recipientName":"ACME","data":{"amount":"10.00","message":"hi"}}""";

    private FlowFixtures() {
    }

    static FaxTask submitTask(long now, long deadline) {
        return FaxTask.newSubmit("MID-1", new Origin("src", 0, 42L, "k", now), Map.of("MID", "MID-1"), PAYLOAD, now,
                deadline, now);
    }

    static FaxTask pollTask(long now, long pollDeadline) {
        return submitTask(now, now + 3_600_000).submitted("job-1", now, pollDeadline, now + 30_000);
    }

    /** Scriptable RightFax. Each supplier may throw. */
    static final class FakeRightFax implements RightFaxOperations {
        Supplier<String> submit = () -> "job-1";
        Supplier<JobStatus> job = () -> new JobStatus("Processing", "InProgress", null);
        Supplier<List<DocumentStatus>> docs = List::of;
        final List<String> calls = new ArrayList<>();

        @Override
        public String submit(byte[] pdf, FaxRequest request) {
            calls.add("submit");
            return submit.get();
        }

        @Override
        public JobStatus jobStatus(String jobId) {
            calls.add("job");
            return job.get();
        }

        @Override
        public List<DocumentStatus> documents(String jobId) {
            calls.add("docs");
            return docs.get();
        }
    }
}
