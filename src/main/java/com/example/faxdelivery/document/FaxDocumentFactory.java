package com.example.faxdelivery.document;

import com.example.faxdelivery.message.FaxRequest;

/** FaxRequest -> PDF bytes. Throws PermanentFailureException on bad data (template or PDF). */
@FunctionalInterface
public interface FaxDocumentFactory {

    byte[] create(FaxRequest request);
}
