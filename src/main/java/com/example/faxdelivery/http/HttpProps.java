package com.example.faxdelivery.http;

import java.time.Duration;

/**
 * HTTP client settings for one downstream.
 *
 * @param connectTimeout        TCP connect timeout (failure = request never sent, always safe to retry)
 * @param responseTimeout       max wait for the response once the request is written (expiry is ambiguous)
 * @param callTimeout           hard cap per call including pool acquisition; must be >= connect + response
 * @param maxConnections        connection pool size
 * @param maxIdleTime           evict idle connections before a load balancer silently drops them
 * @param pendingAcquireTimeout max wait for a pooled connection
 * @param maxInMemorySize       max response size buffered in memory
 */
public record HttpProps(Duration connectTimeout, Duration responseTimeout, Duration callTimeout,
                        Integer maxConnections, Duration maxIdleTime, Duration pendingAcquireTimeout,
                        Integer maxInMemorySize) {

    public HttpProps {
        connectTimeout = connectTimeout == null ? Duration.ofSeconds(5) : connectTimeout;
        responseTimeout = responseTimeout == null ? Duration.ofSeconds(25) : responseTimeout;
        callTimeout = callTimeout == null ? Duration.ofSeconds(30) : callTimeout;
        maxConnections = maxConnections == null ? 10 : maxConnections;
        maxIdleTime = maxIdleTime == null ? Duration.ofSeconds(30) : maxIdleTime;
        pendingAcquireTimeout = pendingAcquireTimeout == null ? Duration.ofSeconds(5) : pendingAcquireTimeout;
        maxInMemorySize = maxInMemorySize == null ? 4 * 1024 * 1024 : maxInMemorySize;
    }
}
