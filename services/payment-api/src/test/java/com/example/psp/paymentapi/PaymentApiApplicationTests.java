package com.example.psp.paymentapi;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Proves the M1 acceptance bar: the app starts standalone, with no database and no Kafka broker
 * available - only spring-boot-starter-web + actuator, plus common-web's auto-configured
 * correlation-id filter and error handler.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PaymentApiApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
