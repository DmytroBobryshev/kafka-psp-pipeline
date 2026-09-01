package com.example.psp.webhooknotifier;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "payments.payment-status-changed.v1",
            "refunds.refund-completed.v1",
            "refunds.refund-failed.v1",
            "refunds.refund-status-changed.v1",
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
