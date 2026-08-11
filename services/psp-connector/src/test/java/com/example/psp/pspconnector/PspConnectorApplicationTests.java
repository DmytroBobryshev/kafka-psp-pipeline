package com.example.psp.pspconnector;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Proves the application context wires up correctly: an embedded Kafka broker stands in for the
 * real compose stack (this test never touches infra/compose), and an in-memory H2 database
 * (PostgreSQL compatibility mode) stands in for the real psp_connector Postgres instance - same
 * pattern as payment-api's PaymentApiApplicationTests. The real end-to-end integration is
 * exercised for real against the live infra/compose stack and captured in README.md, not
 * reproduced here.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "payments.payment-requested.v1",
            // M11: refunds.funds-reserved.v1 is consumed; refund-completed/refund-failed are
            // produced (config.RefundKafkaConsumerConfig, adapters.out.kafka.KafkaRefundStatusPublisher).
            "refunds.funds-reserved.v1",
            "refunds.refund-completed.v1",
            "refunds.refund-failed.v1"
        })
class PspConnectorApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
