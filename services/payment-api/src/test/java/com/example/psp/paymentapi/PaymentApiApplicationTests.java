package com.example.psp.paymentapi;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "psp.provider-status-query.v1",
            "psp.provider-status-reply.v1",
            "payments.payment-status-changed.v1",
            // config.MerchantViewKafkaConfig's merchant-view listener container, same requirement.
            "merchants.merchant-config-changed.v1",
            "refunds.refund-status-changed.v1",
            "refunds.refund-completed.v1",
            "refunds.refund-failed.v1",
            "refunds.funds-reserved.v1"
        })
class PaymentApiApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
