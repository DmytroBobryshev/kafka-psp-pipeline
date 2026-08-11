package com.example.psp.realtimegateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Proves the application context wires up correctly: an embedded Kafka broker stands in for the
 * real compose stack (this test never touches infra/compose). No database - this service has
 * none. The real end-to-end integration (a real payment event arriving over SSE) is exercised for
 * real against the live infra/compose stack and captured in README.md, not reproduced here.
 */
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
