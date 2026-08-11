package com.example.psp.analytics;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Proves the application context wires up correctly - which for this service means proving that
 * the topology can actually be <b>described</b>: {@code adapters.in.kafka.AnalyticsTopology}'s
 * constructor runs at context refresh and calls the full {@code StreamsBuilder} DSL chain, so a
 * missing Serde bean, an invalid {@code Materialized} (for instance a store retention shorter
 * than {@code windowSize + grace}), or a store name collision fails here rather than at deploy
 * time.
 *
 * <p>What it deliberately does NOT do is start the Streams client:
 * {@code spring.kafka.streams.auto-startup=false} (test resources {@code application.yml}). A
 * single embedded broker cannot satisfy the internal topics' {@code replication.factor=3}, and
 * this test is not the place to weaken that - the real thing runs against the three-broker
 * compose stack and is captured in README.md.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 1,
        topics = {"payments.payment-status-changed.v1", "merchants.merchant-config-changed.v1"})
class AnalyticsApplicationTests {

    @Test
    void contextLoads() {
        // Intentionally empty: a failing ApplicationContext fails this test.
    }
}
