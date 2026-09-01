package com.example.psp.ledger.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ledger")
public record LedgerProperties(String instanceId, Kafka kafka) {

    public record Kafka(
            String paymentStatusChangedTopic,
            String ledgerEntryRecordedTopic,
            String transactionalIdPrefix) {

        public Kafka {
            if (transactionalIdPrefix == null || transactionalIdPrefix.isBlank()) {
                throw new IllegalArgumentException(
                        "ledger.kafka.transactional-id-prefix must not be blank - a transactional "
                                + "producer without a stable transactional.id cannot fence zombies");
            }
        }
    }
}
