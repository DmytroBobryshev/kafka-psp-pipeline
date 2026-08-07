package com.example.psp.paymentapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * M1 walking skeleton entry point. Starts standalone: no database, no Kafka broker required
 * (spring-boot-starter-web + actuator only). {@code common-web}'s correlation-id filter and
 * RFC-7807 error handler register themselves via Spring Boot auto-configuration - see {@code
 * com.example.psp.common.web.config.CommonWebAutoConfiguration}.
 */
@SpringBootApplication
public class PaymentApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentApiApplication.class, args);
    }
}
