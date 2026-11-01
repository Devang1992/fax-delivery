package com.example.faxdelivery.listener;

import java.util.List;

/** Listener container ids (also used by the breaker to pause/resume them) and container factory bean names. */
public final class ListenerIds {

    public static final String SOURCE = "fax-source";
    public static final String RETRY = "fax-retry";
    public static final String POLL = "fax-poll";

    public static final List<String> ALL = List.of(SOURCE, RETRY, POLL);

    public static final String SOURCE_FACTORY = "faxSourceContainerFactory";
    public static final String RETRY_FACTORY = "faxRetryContainerFactory";
    public static final String POLL_FACTORY = "faxPollContainerFactory";

    private ListenerIds() {
    }
}
