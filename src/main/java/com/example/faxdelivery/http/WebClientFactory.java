package com.example.faxdelivery.http;

import java.time.Duration;

import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;

import io.netty.channel.ChannelOption;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/** One WebClient per downstream, each with its own bounded connection pool. */
public final class WebClientFactory {

    private WebClientFactory() {
    }

    public static WebClient create(String poolName, String baseUrl, HttpProps p) {
        ConnectionProvider provider = ConnectionProvider.builder(poolName)
                .maxConnections(p.maxConnections())
                .maxIdleTime(p.maxIdleTime())
                .pendingAcquireTimeout(p.pendingAcquireTimeout())
                .evictInBackground(Duration.ofSeconds(30))
                .build();
        HttpClient httpClient = HttpClient.create(provider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) p.connectTimeout().toMillis())
                .responseTimeout(p.responseTimeout());
        return WebClient.builder()
                .baseUrl(baseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(c -> c.defaultCodecs().maxInMemorySize(p.maxInMemorySize()))
                .build();
    }
}
