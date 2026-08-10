package com.example.psp.ledger;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Proves the application context wires up correctly - which, for this service, means proving that
 * the two transaction managers, the transactional producer factory and the transaction-aware
 * listener container all coexist. That is not a trivial assertion: defining a
 * {@code KafkaTransactionManager} in a user configuration class suppresses Spring Boot's
 * auto-configured JPA transaction manager ({@code @ConditionalOnMissingBean(TransactionManager)}),
 * and Spring Data JPA then fails to find the bean named {@code transactionManager}. This test is
 * what catches that regression if the explicit declaration in
 * {@code config.KafkaProducerConfig#transactionManager} is ever removed.
 *
 * <p>An embedded broker stands in for the compose stack and H2 (PostgreSQL mode) for the real
 * {@code ledger} database. No transaction is actually begun here - no records are published - so
 * the single embedded broker never needs to create {@code __transaction_state}; the broker
 * properties below make that topic creatable anyway rather than relying on that being true.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {"payments.payment-status-changed.v1", "ledger.ledger-entry-recorded.v1"},
        brokerProperties = {
            // A single embedded broker cannot satisfy the production defaults (RF 3 / min ISR 2)
            // for the internal transaction-state topic.
            "transaction.state.log.replication.factor=1",
            "transaction.state.log.min.isr=1",
            "offsets.topic.replication.factor=1"
        })
class LedgerApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
