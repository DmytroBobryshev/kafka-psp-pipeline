package com.example.psp.paymentapi;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Proves the M1 acceptance bar: the app starts standalone, with no database available - only
 * spring-boot-starter-web + actuator, plus common-web's auto-configured correlation-id filter and
 * error handler.
 *
 * <p>M12 changed the "no Kafka broker available" half of that claim: {@code
 * config.ReplyingKafkaConfig}'s {@code ReplyingKafkaTemplate} owns a REAL {@code
 * KafkaMessageListenerContainer} (the reply-topic consumer) which, as a {@code SmartLifecycle}
 * bean, auto-starts and begins polling the moment the context refreshes - this is payment-api's
 * first ever real Kafka consumer (see that class's javadoc). An embedded broker now stands in for
 * the real compose stack, same pattern psp-connector's equivalent test already established for
 * the same reason (a real {@code @KafkaListener}/listener-container needs somewhere to connect).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {
            // M12: request-reply. The query topic doesn't strictly need to exist for the
            // producer factory (constructed but never sent from in this test), but the reply
            // topic DOES - the reply container subscribes to it at context-refresh time.
            "psp.provider-status-query.v1",
            "psp.provider-status-reply.v1",
            // M19: config.PaymentStatusViewKafkaConfig's status-view listener container also
            // subscribes at context-refresh time - same requirement as the M12 reply topic above.
            "payments.payment-status-changed.v1"
        })
class PaymentApiApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
