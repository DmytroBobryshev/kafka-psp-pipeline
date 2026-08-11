package com.example.psp.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * M7 entry point: consumes {@code payments.payment-status-changed.v1} inside a Kafka transaction,
 * maintains per-merchant balances in its own Postgres database, and publishes
 * {@code ledger.ledger-entry-recorded.v1}.
 *
 * <p>The two mechanisms that make this correct are separate and must stay separate - Kafka
 * exactly-once for consume-offset-commit + produce, Postgres idempotency for the balance write.
 * See {@code README.md}, in particular "Where Kafka EOS ends".
 *
 * <p>M11 adds the refund saga's three transactional listeners (fitting M7's existing machinery,
 * not bypassing it - see {@code config.RefundKafkaConsumerConfig}) and {@code @EnableScheduling}
 * for {@code adapters.in.scheduler.ReservationTtlSweeper}, the TTL sweep ADR-0008 rule 6 requires.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class LedgerApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgerApplication.class, args);
    }
}
