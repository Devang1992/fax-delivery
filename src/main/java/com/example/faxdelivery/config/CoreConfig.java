package com.example.faxdelivery.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import tools.jackson.databind.json.JsonMapper;

/** Jackson 3 mapper (Boot 4 default) and the clock every time decision uses (replaceable in tests). */
@Configuration(proxyBeanMethods = false)
public class CoreConfig {

    @Bean
    JsonMapper jsonMapper() {
        return JsonMapper.builder().build();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
