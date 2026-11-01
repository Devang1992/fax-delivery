package com.example.faxdelivery.config;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import com.example.faxdelivery.error.HttpFailureClassifier;
import com.example.faxdelivery.http.WebClientFactory;
import com.example.faxdelivery.rightfax.ConditionResolver;
import com.example.faxdelivery.rightfax.RightFaxApi;
import com.example.faxdelivery.rightfax.RightFaxGateway;
import com.example.faxdelivery.rightfax.RightFaxOperations;
import com.example.faxdelivery.rightfax.WebClientRightFaxApi;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
public class RightFaxConfig {

    @Bean
    WebClient rightFaxWebClient(RightFaxProps props) {
        return WebClientFactory.create("rightfax", props.baseUrl(), props.http());
    }

    /** Raw REST calls. Replace this bean to plug in another client (e.g. an existing RFInjector login). */
    @Bean
    RightFaxApi rightFaxApi(@Qualifier("rightFaxWebClient") WebClient webClient, RightFaxProps props,
                            JsonMapper jsonMapper) {
        return new WebClientRightFaxApi(webClient, props, jsonMapper);
    }

    @Bean
    RightFaxOperations rightFaxOperations(RightFaxApi api,
                                          @Qualifier("rightFaxCircuitBreaker") CircuitBreaker breaker,
                                          RightFaxProps props, DocumentProps documentProps, Clock clock) {
        return new RightFaxGateway(api, breaker, new HttpFailureClassifier(props.permanentStatuses()), props,
                documentProps, clock);
    }

    @Bean
    ConditionResolver conditionResolver(RightFaxProps props) {
        return new ConditionResolver(props.conditions().success(), props.conditions().failure());
    }
}
