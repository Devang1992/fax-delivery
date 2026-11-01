package com.example.faxdelivery.rightfax;

/** Call 4a result. Any field may be null if RightFax did not return it. */
public record JobStatus(String condition, String status, String statusText) {

    public String describe() {
        return "condition=" + condition + " status=" + status + (statusText == null ? "" : " statusText=\"" + statusText + "\"");
    }
}
