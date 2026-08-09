package com.example.psp.pspconnector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * M4 entry point: consumes {@code payments.payment-requested.v1}, simulates a payment provider,
 * publishes {@code payments.payment-status-changed.v1}. See {@code README.md} for architecture,
 * "prove it" experiments, and how to run against the {@code infra/compose} stack.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class PspConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(PspConnectorApplication.class, args);
    }
}
