package com.example.psp.webhooknotifier;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Proves the application context wires up correctly: an embedded Kafka broker stands in for the
 * real compose stack (same pattern as every other service's equivalent test), and
 * {@code config.MongoIndexConfig}'s TTL-index initializer is disabled via
 * {@code webhook-notifier.mongo.create-ttl-index-on-startup=false} (test resources
 * {@code application.yml}) so this test needs no live MongoDB either - MongoDB's own Spring Boot
 * autoconfiguration does not eagerly validate connectivity the way a JDBC {@code DataSource}
 * does, but that ApplicationRunner does perform a real round trip, hence the explicit disable.
 * The real end-to-end integration is exercised for real against the live infra/compose stack and
 * captured in README.md, not reproduced here.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "payments.payment-status-changed.v1",
            // M19: config.KafkaConsumerConfig's refundCompletedKafkaListenerContainerFactory /
            // refundFailedKafkaListenerContainerFactory also subscribe at context-refresh time.
            "refunds.refund-completed.v1",
            "refunds.refund-failed.v1",
            // M24: refundStatusChangedKafkaListenerContainerFactory also subscribes at
            // context-refresh time (adapters.in.kafka.RefundExpiredListener).
            "refunds.refund-status-changed.v1",
            // Merchant webhook projection: config.KafkaConsumerConfig's
            // merchantViewKafkaListenerContainerFactory also subscribes at context-refresh time.
            "merchants.merchant-config-changed.v1",
            "webhooks.webhook-delivery-requested.v1",
            "webhooks.webhook-delivery-requested.v1.retry.5s",
            "webhooks.webhook-delivery-requested.v1.retry.1m",
            "webhooks.webhook-delivery-requested.v1.retry.15m",
            "webhooks.webhook-delivery-requested.v1.dlq"
        })
class WebhookNotifierApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
