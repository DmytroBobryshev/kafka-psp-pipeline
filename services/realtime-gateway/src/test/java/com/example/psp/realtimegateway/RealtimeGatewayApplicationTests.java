package com.example.psp.realtimegateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "payments.payment-requested.v1",
            "payments.payment-status-changed.v1",
            "refunds.refund-requested.v1",
            "refunds.funds-reserved.v1",
            "refunds.refund-completed.v1",
            "refunds.refund-failed.v1",
            "refunds.reservation-released.v1"
        })
class RealtimeGatewayApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
