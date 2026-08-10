package com.example.psp.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * M7 entry point: consumes {@code payments.payment-status-changed.v1} inside a Kafka transaction,
 * maintains per-merchant balances in its own Postgres database, and publishes
 * {@code ledger.ledger-entry-recorded.v1}.
 *
 * <p>The two mechanisms that make this correct are separate and must stay separate - Kafka
 * exactly-once for consume-offset-commit + produce, Postgres idempotency for the balance write.
 * See {@code README.md}, in particular "Where Kafka EOS ends".
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class LedgerApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgerApplication.class, args);
    }
}
