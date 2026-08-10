package com.example.psp.ledger.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code ledger.*} from {@code application.yml}.
 *
 * <p>{@code ledger.fail-after-produce} is deliberately <b>not</b> bound here: it is injected
 * straight into {@code application.RecordLedgerEntryUseCase} with {@code @Value} so that
 * {@code application/} does not acquire a dependency on {@code config/} for one boolean. It is
 * still documented in {@code application.yml} and in the README.
 *
 * @param instanceId identity of this logical ledger instance. Drives
 *                   {@link Kafka#transactionalIdPrefix()} and MUST be stable across restarts of
 *                   the same instance and distinct between instances - see
 *                   {@link KafkaProducerConfig}'s javadoc, section 3, for what breaks otherwise
 *                   (zombie fencing, silently). On Kubernetes (M18) this is the StatefulSet
 *                   ordinal; locally, a {@code --ledger.instance-id=1} override when running a
 *                   second copy.
 */
@ConfigurationProperties(prefix = "ledger")
public record LedgerProperties(String instanceId, Kafka kafka) {

    /**
     * @param paymentStatusChangedTopic   inbound topic (ADR-0003: keyed by {@code merchantId}, so
     *                                    the ledger has a single in-flight writer per balance).
     * @param ledgerEntryRecordedTopic    outbound topic, produced transactionally.
     * @param transactionalIdPrefix       the fencing-critical value. See
     *                                    {@link KafkaProducerConfig}.
     */
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
