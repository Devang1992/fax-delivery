package com.example.faxdelivery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Fax delivery over OpenText RightFax.
 *
 * <pre>
 * source topic --(PREFERENCE=FAX)--> SUBMIT (render, PDF, upload, send) --> fax poll topic --> DELIVERED / FAILED
 *                                         |                                      ^
 *                                         +-- transient --> fax retry topic -----+
 * </pre>
 *
 * Three topics only (source, retry, poll). One RightFax circuit breaker; while it is OPEN every fax
 * consumer is paused and nothing is lost or requeued. See DESIGN.md.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class FaxDeliveryApplication {

    public static void main(String[] args) {
        SpringApplication.run(FaxDeliveryApplication.class, args);
    }
}
