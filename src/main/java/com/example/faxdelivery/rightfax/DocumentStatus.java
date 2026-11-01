package com.example.faxdelivery.rightfax;

/** One document of a job (call 4b). */
public record DocumentStatus(String id, String condition, String status, String statusText) {

    public String describe() {
        return "document=" + id + " condition=" + condition + " status=" + status
                + (statusText == null ? "" : " statusText=\"" + statusText + "\"");
    }
}
