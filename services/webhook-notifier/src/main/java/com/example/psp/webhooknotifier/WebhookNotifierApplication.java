package com.example.psp.webhooknotifier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * M8 entry point: consumes {@code payments.payment-status-changed.v1} and plans one webhook
 * delivery per event ({@code adapters.in.kafka.PaymentStatusChangedListener}); consumes
 * {@code webhooks.webhook-delivery-requested.v1} (plus its three retry tiers) and attempts the
 * HTTP callback ({@code adapters.in.kafka.WebhookDeliveryExecutorListener}); exposes a simulated
 * merchant endpoint and a DLQ replay endpoint. See {@code README.md} for architecture, the retry
 * topology, the error taxonomy, every configurable knob, and how to run against the
 * {@code infra/compose} stack.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class WebhookNotifierApplication {

    public static void main(String[] args) {
        SpringApplication.run(WebhookNotifierApplication.class, args);
    }
}
