package com.example.psp.pspconnector;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {
            "payments.payment-requested.v1",
            "refunds.funds-reserved.v1",
            "refunds.refund-completed.v1",
            "refunds.refund-failed.v1",
            "psp.provider-status-query.v1",
            "psp.provider-status-reply.v1"
        })
class PspConnectorApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
