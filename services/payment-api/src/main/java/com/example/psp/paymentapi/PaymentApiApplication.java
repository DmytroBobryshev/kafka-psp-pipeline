package com.example.psp.paymentapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * M1 walking skeleton entry point. Starts standalone: no database, no Kafka broker required
 * (spring-boot-starter-web + actuator only). {@code common-web}'s correlation-id filter and
 * RFC-7807 error handler register themselves via Spring Boot auto-configuration - see {@code
 * com.example.psp.common.web.config.CommonWebAutoConfiguration}.
 *
 * <p>{@code @EnableScheduling} (M22) activates {@code adapters.in.scheduler.
 * PaymentExpirationScheduler}'s {@code @Scheduled} sweep - same annotation, same reasoning, as
 * ledger's {@code LedgerApplication} for its own TTL sweeper.
 */
@SpringBootApplication
@EnableScheduling
public class PaymentApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentApiApplication.class, args);
    }
}
