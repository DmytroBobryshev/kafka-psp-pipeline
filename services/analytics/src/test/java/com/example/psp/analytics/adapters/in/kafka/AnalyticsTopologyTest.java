package com.example.psp.analytics.adapters.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.analytics.application.ProjectWindowMetricsUseCase;
import com.example.psp.analytics.config.AnalyticsProperties;
import com.example.psp.analytics.config.StreamsStores;
import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import com.example.psp.analytics.domain.model.MerchantWindowMetrics;
import com.example.psp.analytics.domain.port.MetricsProjectionRepository;
import com.example.psp.common.events.avro.EventEnvelope;
import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the <b>real</b> topology (M10) under {@code TopologyTestDriver} - no broker, no Schema
 * Registry (a {@code mock://} URL gives Confluent's serdes an in-memory registry), no Mongo.
 *
 * <p>This is where the module's behavioural claims are actually asserted rather than described:
 * tumbling window boundaries, the grace period accepting one late record and dropping a later
 * one, {@code leftJoin} keeping payments for unconfigured merchants, and - the headline - a
 * <b>tombstone removing the row from the {@code GlobalKTable}</b> with no tombstone-handling code
 * anywhere in the topology.
 *
 * <p>{@code statestore.cache.max.bytes=0} below is the standard {@code TopologyTestDriver}
 * setting and also a demonstration: with the cache off, every input record produces a downstream
 * emit, which is exactly the behaviour {@code config.KafkaStreamsConfig}'s comment warns about for
 * the Mongo projection.
 */
class AnalyticsTopologyTest {

    private static final String PAYMENTS_TOPIC = "payments.payment-status-changed.v1";
    private static final String CONFIG_TOPIC = "merchants.merchant-config-changed.v1";
    private static final String MERCHANT = "acme";

    /** 12:00:00Z exactly - a tumbling-window boundary, so window maths is readable. */
    private static final Instant BASE = Instant.parse("2026-08-11T12:00:00Z");

    @TempDir private Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, PaymentStatusChanged> payments;
    private TestInputTopic<String, MerchantConfigChanged> merchantConfig;
    private RecordingProjectionRepository projections;

    @BeforeEach
    void setUp() {
        String registryUrl = "mock://analytics-topology-test-" + UUID.randomUUID();

        SpecificAvroSerde<PaymentStatusChanged> paymentSerde = avroSerde(registryUrl);
        SpecificAvroSerde<MerchantConfigChanged> configSerde = avroSerde(registryUrl);

        AnalyticsProperties properties =
                new AnalyticsProperties(
                        new AnalyticsProperties.Kafka(PAYMENTS_TOPIC, CONFIG_TOPIC),
                        new AnalyticsProperties.SchemaRegistry(registryUrl),
                        new AnalyticsProperties.Streams(
                                "analytics-streams.topology-test",
                                stateDir.toString(),
                                1,
                                StreamsConfig.AT_LEAST_ONCE,
                                "localhost:8089",
                                Duration.ofSeconds(5),
                                0L),
                        new AnalyticsProperties.Windows(
                                Duration.ofMinutes(1),
                                Duration.ofSeconds(30),
                                Duration.ofMinutes(15),
                                Duration.ofMinutes(5)));

        projections = new RecordingProjectionRepository();

        StreamsBuilder builder = new StreamsBuilder();
        AnalyticsTopology.define(
                builder,
                properties,
                paymentSerde,
                configSerde,
                new ProjectWindowMetricsUseCase(projections),
                Clock.fixed(BASE, ZoneOffset.UTC));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "analytics-streams.topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        // Forward every update immediately - see the class javadoc.
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(builder.build(), config);
        payments = driver.createInputTopic(PAYMENTS_TOPIC, new StringSerializer(), paymentSerde.serializer());
        merchantConfig =
                driver.createInputTopic(CONFIG_TOPIC, new StringSerializer(), configSerde.serializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    void aggregatesOneMinuteTumblingWindowsAndEnrichesThemFromTheGlobalKTable() {
        merchantConfig.pipeInput(MERCHANT, config("ACME Corp", 1500));

        payments.pipeInput(MERCHANT, payment("SUCCEEDED", BASE.plusSeconds(5)));
        payments.pipeInput(MERCHANT, payment("DECLINED", BASE.plusSeconds(10)));
        payments.pipeInput(MERCHANT, payment("SUCCEEDED", BASE.plusSeconds(20)));

        MerchantMetricsWindow window = projections.latestFor(MERCHANT, BASE);
        assertThat(window.windowStart()).isEqualTo(BASE);
        assertThat(window.windowEnd()).isEqualTo(BASE.plusSeconds(60));
        assertThat(window.metrics().totalCount()).isEqualTo(3L);
        assertThat(window.metrics().declinedCount()).isEqualTo(1L);
        assertThat(window.metrics().declineRateBps()).isEqualTo(3333L);
        // The join is why these two are here at all.
        assertThat(window.metrics().merchantDisplayName()).isEqualTo("ACME Corp");
        assertThat(window.metrics().declineRateAlert()).isTrue();
    }

    @Test
    void aRecordAfterTheWindowEndsStartsANewWindowRatherThanExtendingTheOldOne() {
        merchantConfig.pipeInput(MERCHANT, config("ACME Corp", 1500));

        payments.pipeInput(MERCHANT, payment("SUCCEEDED", BASE.plusSeconds(30)));
        payments.pipeInput(MERCHANT, payment("SUCCEEDED", BASE.plusSeconds(75))); // 12:01:15

        assertThat(projections.latestFor(MERCHANT, BASE).metrics().totalCount()).isEqualTo(1L);
        assertThat(projections.latestFor(MERCHANT, BASE.plusSeconds(60)).metrics().totalCount())
                .isEqualTo(1L);
    }

    @Test
    void gracePeriodAcceptsALateRecordAndThenStopsAcceptingIt() {
        merchantConfig.pipeInput(MERCHANT, config("ACME Corp", 1500));

        payments.pipeInput(MERCHANT, payment("SUCCEEDED", BASE.plusSeconds(10)));

        // Advance stream time to 12:01:20 - past the first window's end (12:01:00) but still
        // inside its 30s grace (until 12:01:30).
        payments.pipeInput(MERCHANT, payment("SUCCEEDED", BASE.plusSeconds(80)));
        // A record whose EVENT time is back in the first window: accepted, because grace has not
        // expired.
        payments.pipeInput(MERCHANT, payment("DECLINED", BASE.plusSeconds(20)));

        assertThat(projections.latestFor(MERCHANT, BASE).metrics().totalCount()).isEqualTo(2L);

        // Now push stream time to 12:02:00, well past 12:01:30, and try the same trick again.
        payments.pipeInput(MERCHANT, payment("SUCCEEDED", BASE.plusSeconds(120)));
        payments.pipeInput(MERCHANT, payment("DECLINED", BASE.plusSeconds(30)));

        // Silently dropped - the first window is closed for good. "Silently" is literal: Streams
        // counts it in the `late-record-drop` metric and emits nothing, which is why a grace
        // period that is too short is invisible unless you look at that metric.
        assertThat(projections.latestFor(MERCHANT, BASE).metrics().totalCount()).isEqualTo(2L);
    }

    @Test
    void leftJoinKeepsPaymentsForAMerchantThatHasNoConfig() {
        // No config published at all for this merchant.
        payments.pipeInput("never-configured", payment("never-configured", "DECLINED", BASE.plusSeconds(5)));

        MerchantMetricsWindow window = projections.latestFor("never-configured", BASE);
        assertThat(window.metrics().totalCount()).isEqualTo(1L);
        assertThat(window.metrics().merchantDisplayName()).isNull();
        // An unknown merchant cannot breach a threshold it does not have - but its volume is NOT
        // lost, which an inner join would have done.
        assertThat(window.metrics().declineRateAlert()).isFalse();
    }

    @Test
    void aTombstoneRemovesTheRowFromTheGlobalKTable() {
        merchantConfig.pipeInput(MERCHANT, config("ACME Corp", 1500));

        KeyValueStore<String, MerchantConfigChanged> configStore =
                driver.getKeyValueStore(StreamsStores.MERCHANT_CONFIG);
        assertThat(configStore.get(MERCHANT)).isNotNull();

        // THE tombstone: same key, null value. Not a flag, not an empty record.
        merchantConfig.pipeInput(MERCHANT, null);

        assertThat(configStore.get(MERCHANT))
                .as("a null value deletes the GlobalKTable row - no tombstone-handling code exists "
                        + "anywhere in AnalyticsTopology, and none is needed")
                .isNull();

        // And the effect is visible in the aggregate: a payment after the tombstone joins nothing.
        payments.pipeInput(MERCHANT, payment("DECLINED", BASE.plusSeconds(5)));
        assertThat(projections.latestFor(MERCHANT, BASE).metrics().merchantDisplayName()).isNull();
    }

    @Test
    void theWindowStoreIsQueryableByNameAndTimeRange() {
        merchantConfig.pipeInput(MERCHANT, config("ACME Corp", 1500));
        payments.pipeInput(MERCHANT, payment("SUCCEEDED", BASE.plusSeconds(5)));

        // Exactly what the interactive-query adapter does at runtime, minus the KafkaStreams
        // lifecycle checks: fetch(key, from, to) on a window store.
        WindowStore<String, MerchantWindowMetrics> store =
                driver.getWindowStore(StreamsStores.MERCHANT_METRICS);

        List<Long> windowStarts = new ArrayList<>();
        try (var iterator = store.fetch(MERCHANT, BASE.minusSeconds(60), BASE.plusSeconds(120))) {
            while (iterator.hasNext()) {
                windowStarts.add(iterator.next().key);
            }
        }

        assertThat(windowStarts).containsExactly(BASE.toEpochMilli());
    }

    // ---------------------------------------------------------------------------------------

    private static <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> avroSerde(
            String registryUrl) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(
                Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl,
                        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true),
                false);
        return serde;
    }

    private static PaymentStatusChanged payment(String status, Instant occurredAt) {
        return payment(MERCHANT, status, occurredAt);
    }

    private static PaymentStatusChanged payment(String merchantId, String status, Instant occurredAt) {
        return PaymentStatusChanged.newBuilder()
                .setEnvelope(envelope("payments.payment-status-changed.v1", merchantId, occurredAt))
                .setPaymentId(UUID.randomUUID().toString())
                .setMerchantId(merchantId)
                .setAmount(new BigDecimal("12.3400"))
                .setCurrency("EUR")
                .setStatus(status)
                .setProviderReference(UUID.randomUUID().toString())
                .setDeclineReason("DECLINED".equals(status) ? "insufficient_funds" : null)
                .build();
    }

    private static MerchantConfigChanged config(String displayName, int thresholdBps) {
        return MerchantConfigChanged.newBuilder()
                .setEnvelope(envelope("merchants.merchant-config-changed.v1", MERCHANT, BASE))
                .setMerchantId(MERCHANT)
                .setDisplayName(displayName)
                .setStatus("ACTIVE")
                .setPayoutCurrency("EUR")
                .setWebhookUrl("https://acme.test/hooks")
                .setDeclineRateAlertThresholdBps(thresholdBps)
                .build();
    }

    private static EventEnvelope envelope(String eventType, String aggregateId, Instant occurredAt) {
        return EventEnvelope.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(eventType)
                .setEventVersion(1)
                .setAggregateId(aggregateId)
                .setAggregateType("payment")
                .setOccurredAt(occurredAt)
                .setSource("test")
                .setTraceId(UUID.randomUUID().toString())
                .setCorrelationId(UUID.randomUUID().toString())
                .setCausationId(null)
                .build();
    }

    /** In-memory stand-in for MongoDB; keyed exactly like the real projection's {@code _id}. */
    private static final class RecordingProjectionRepository implements MetricsProjectionRepository {

        private final Map<String, MerchantMetricsWindow> byKey = new LinkedHashMap<>();

        @Override
        public void save(MerchantMetricsWindow window) {
            byKey.put(window.key(), window);
        }

        @Override
        public List<MerchantMetricsWindow> findByMerchantSince(String merchantId, Instant from) {
            return byKey.values().stream()
                    .filter(w -> w.merchantId().equals(merchantId) && !w.windowStart().isBefore(from))
                    .toList();
        }

        MerchantMetricsWindow latestFor(String merchantId, Instant windowStart) {
            MerchantMetricsWindow window = byKey.get(merchantId + "|" + windowStart.toEpochMilli());
            assertThat(window)
                    .as("no projection for merchantId=%s windowStart=%s; have %s", merchantId, windowStart, byKey.keySet())
                    .isNotNull();
            return window;
        }
    }
}
