package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.analytics.application.ProjectWindowMetricsUseCase;
import com.example.psp.analytics.config.AnalyticsProperties;
import com.example.psp.analytics.config.StreamsStores;
import com.example.psp.analytics.domain.model.MerchantWindowMetrics;
import com.example.psp.analytics.domain.model.PaymentOutcome;
import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import java.time.Clock;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

/**
 * The topology (M10). One sub-topology, four interesting decisions.
 *
 * <pre>
 *   merchants.merchant-config-changed.v1 (compacted, 3 partitions, Avro)
 *        |
 *        +--> GlobalKTable "merchant-config-store"  (every partition on every instance)
 *                 ^
 *                 | leftJoin, keyed by merchantId
 *                 |
 *   payments.payment-status-changed.v1 (12 partitions, key=merchantId, Avro)
 *        |        |
 *        +--> KStream --> [join] --> groupByKey --> 1-min tumbling window (+30s grace)
 *                                       |
 *                                       +--> WindowStore "merchant-metrics-1m"
 *                                       |         (changelog: analytics-streams.v1-
 *                                       |          merchant-metrics-1m-changelog)
 *                                       +--> toStream --> foreach --> MongoDB projection
 * </pre>
 *
 * <h2>1. GlobalKTable, not KTable</h2>
 *
 * <p>A {@code KStream x KTable} join requires the two topics to be <b>co-partitioned</b>: same
 * number of partitions, same partitioning strategy, same key. That is a real constraint here and
 * it is violated on the first count -
 * {@code payments.payment-status-changed.v1} has <b>12</b> partitions and
 * {@code merchants.merchant-config-changed.v1} has <b>3</b> (ADR-0003, and 3 is correct: the topic
 * is low-volume config). A {@code KTable} join would fail the co-partitioning check at startup,
 * and the fixes are all bad: raise the config topic to 12 partitions (churning a compacted topic's
 * key-to-partition mapping, the one thing ADR-0003 warns is effectively irreversible), or
 * repartition the 12-partition payment stream down to 3 (throwing away the parallelism the
 * payment path was sized for).
 *
 * <p>A {@code GlobalKTable} sidesteps the constraint by not being partitioned at all: <b>every</b>
 * instance consumes <b>every</b> partition of the source topic into a full local copy, so a lookup
 * by any key succeeds on any instance. That is what makes the join a plain local dictionary
 * lookup, and it also makes the join non-shuffling: no repartition topic, no key change, no
 * network hop.
 *
 * <p>What it costs, honestly:
 *
 * <ul>
 *   <li><b>Full replication.</b> Every instance holds every merchant. Fine for a config table
 *       (thousands of rows, kilobytes); catastrophic for something merchant-sized x
 *       payment-sized. The rule of thumb is that a GlobalKTable's source must be small enough
 *       that N copies is not a design decision.</li>
 *   <li><b>No per-task time synchronisation.</b> A GlobalKTable is fed by a dedicated global
 *       thread that reads as fast as it can, independent of stream time. The join therefore uses
 *       "whatever config is in the table right now", not "the config as of the payment's event
 *       time" - so replaying history does NOT faithfully reproduce the config that was in force
 *       back then. A KTable-KStream join is timestamp-synchronised and would. This is a real
 *       correctness difference and it is accepted here: the metrics label a window with the
 *       merchant's <i>current</i> name and threshold, which is what a dashboard wants.</li>
 *   <li><b>Bootstrap cost.</b> The global store is fully restored before the application reaches
 *       RUNNING, so startup is gated on reading the whole compacted topic. Compaction is exactly
 *       what keeps that bounded: the topic's length is O(merchants), not O(config changes).</li>
 * </ul>
 *
 * <h2>2. leftJoin, not join</h2>
 *
 * <p>An inner join would <b>silently drop</b> every payment whose merchant has no config - which
 * is precisely what a tombstone produces, and precisely what a merchant onboarded through
 * psp-connector before anyone called {@code PUT /api/merchants/{id}/config} produces. A metrics
 * pipeline that responds to "config missing" by making revenue disappear from the dashboard is
 * worse than useless. {@code leftJoin} keeps the payment and passes {@code null} for the config;
 * {@link PaymentOutcome#merchantConfigKnown()} carries that fact forward instead of hiding it.
 *
 * <h2>3. No repartition topic</h2>
 *
 * <p>{@code groupByKey()} - not {@code groupBy(...)}. The source topic is already keyed by
 * {@code merchantId} (ADR-0003, "analytics aggregates per merchant with no repartition topic on
 * the main path (M10)"), and nothing between the source and the grouping changes the key: the
 * GlobalKTable join preserves it, and there is no {@code selectKey}/{@code map} anywhere. Streams
 * therefore knows the data is already partitioned by the grouping key and inserts no shuffle.
 * Swapping {@code groupByKey()} for an equivalent-looking {@code groupBy((k, v) -> k)} would
 * create {@code analytics-streams.v1-...-repartition} immediately - {@code groupBy} sets the
 * "repartition required" flag unconditionally, because it cannot know the mapper is the identity.
 *
 * <h2>4. No suppress()</h2>
 *
 * <p>{@code suppress(untilWindowCloses(...))} would emit each window exactly once, when it
 * closes. It is not used here for two reasons: it adds a second, unbounded in-memory (or
 * changelogged) buffer to hold every open window, and it delays every result by
 * {@code windowSize + grace} - 90 s - which would make the interactive query useless for its main
 * job, showing the window that is happening <i>now</i>. The cost is that the Mongo projection
 * sees intermediate results; the projection is a whole-document replace keyed on
 * {@code merchantId|windowStart}, so intermediates converge on the final value rather than
 * accumulating.
 */
@Component
public class AnalyticsTopology {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsTopology.class);

    private static final String DECLINED = "DECLINED";

    public AnalyticsTopology(
            StreamsBuilder streamsBuilder,
            AnalyticsProperties properties,
            io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde<PaymentStatusChanged>
                    paymentStatusChangedSerde,
            io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde<MerchantConfigChanged>
                    merchantConfigChangedSerde,
            ProjectWindowMetricsUseCase projectWindowMetricsUseCase,
            Clock clock) {

        define(
                streamsBuilder,
                properties,
                paymentStatusChangedSerde,
                merchantConfigChangedSerde,
                projectWindowMetricsUseCase,
                clock);

        log.info(
                "Topology defined: window={} grace={} storeRetention={} stores=[{}, {}]",
                properties.windows().size(),
                properties.windows().grace(),
                properties.windows().storeRetention(),
                StreamsStores.MERCHANT_METRICS,
                StreamsStores.MERCHANT_CONFIG);
    }

    /**
     * Static so the topology can be built and exercised by {@code TopologyTestDriver} without a
     * Spring context - see {@code AnalyticsTopologyTest}.
     */
    public static void define(
            StreamsBuilder builder,
            AnalyticsProperties properties,
            Serde<PaymentStatusChanged> paymentStatusChangedSerde,
            Serde<MerchantConfigChanged> merchantConfigChangedSerde,
            ProjectWindowMetricsUseCase projectWindowMetricsUseCase,
            Clock clock) {

        // JSON, not Avro, for the two internal value formats. PaymentOutcome is never serialized
        // at all (no repartition between the join and the aggregation), and MerchantWindowMetrics
        // is written only to the changelog - keeping that human-readable in AKHQ is worth more
        // than registry governance on a topic no other application reads. `noTypeInfo` strips the
        // Java-FQCN type headers Spring's serializer adds by default, which ADR-0002's header
        // contract does not permit and which a state store would never read anyway.
        Serde<PaymentOutcome> paymentOutcomeSerde =
                new JsonSerde<>(PaymentOutcome.class).noTypeInfo();
        Serde<MerchantWindowMetrics> metricsSerde =
                new JsonSerde<>(MerchantWindowMetrics.class).noTypeInfo();

        // ---- source 1: the compacted config topic, as a fully-replicated global table ----------
        GlobalKTable<String, MerchantConfigChanged> merchantConfig =
                builder.globalTable(
                        properties.kafka().merchantConfigChangedTopic(),
                        Consumed.with(Serdes.String(), merchantConfigChangedSerde)
                                .withName("merchant-config-source"),
                        // Materialized(...) names the store, which is what makes it queryable by
                        // name from the interactive-query adapter. A global store is implicitly
                        // NON-LOGGED: its source topic is already a compacted changelog, so
                        // Streams creates no ...-merchant-config-store-changelog topic. Calling
                        // withLoggingDisabled() here would be redundant, and calling
                        // withLoggingEnabled() is rejected outright.
                        Materialized.<String, MerchantConfigChanged, KeyValueStore<Bytes, byte[]>>as(
                                        StreamsStores.MERCHANT_CONFIG)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(merchantConfigChangedSerde));

        // ---- source 2: the payment status stream, on EVENT time --------------------------------
        KStream<String, PaymentStatusChanged> payments =
                builder.stream(
                        properties.kafka().paymentStatusChangedTopic(),
                        Consumed.with(Serdes.String(), paymentStatusChangedSerde)
                                .withTimestampExtractor(new EnvelopeEventTimeExtractor())
                                .withName("payment-status-changed-source"));

        // ---- the join: stream x global table, keyed by merchantId ------------------------------
        // The KeyValueMapper below is what a GlobalKTable join has and a KTable join does not: an
        // explicit "given this stream record, which table key do I want?". Here it is the identity
        // on the key, but it is the same hook that would let a stream keyed by paymentId look up a
        // merchant - without any repartitioning, which is the other reason GlobalKTable joins are
        // reached for.
        KStream<String, PaymentOutcome> outcomes =
                payments.leftJoin(
                        merchantConfig,
                        (merchantId, payment) -> merchantId,
                        (payment, config) -> toOutcome(payment, config, clock),
                        Named.as("merchant-config-join"));

        // ---- the windowed aggregation ----------------------------------------------------------
        KTable<Windowed<String>, MerchantWindowMetrics> windowed =
                outcomes
                        // groupByKey, NOT groupBy - see the class javadoc, section 3.
                        .groupByKey(
                                Grouped.with("merchant-outcomes", Serdes.String(), paymentOutcomeSerde))
                        .windowedBy(
                                TimeWindows.ofSizeAndGrace(
                                        properties.windows().size(), properties.windows().grace()))
                        .aggregate(
                                MerchantWindowMetrics::empty,
                                (merchantId, outcome, aggregate) -> aggregate.plus(outcome),
                                Named.as("merchant-metrics-1m-aggregate"),
                                Materialized
                                        .<String, MerchantWindowMetrics, WindowStore<Bytes, byte[]>>as(
                                                StreamsStores.MERCHANT_METRICS)
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(metricsSerde)
                                        // Retention decides two things at once: how far back an
                                        // interactive query can see, and how much RocksDB (and
                                        // changelog) is on disk. The floor is size + grace, below
                                        // which Streams refuses to build the store because a window
                                        // could expire before its grace period ends.
                                        .withRetention(properties.windows().storeRetention()));

        // ---- terminal: project to MongoDB -------------------------------------------------------
        windowed
                .toStream(Named.as("merchant-metrics-1m-to-stream"))
                .foreach(
                        (windowedKey, metrics) -> {
                            if (metrics == null) {
                                // Defensive: a windowed KTable emits null when a window's state is
                                // dropped. Nothing in this topology produces that today, but a
                                // future suppress()/retention change could.
                                return;
                            }
                            projectWindowMetricsUseCase.project(
                                    windowedKey.key(),
                                    windowedKey.window().startTime(),
                                    windowedKey.window().endTime(),
                                    metrics);
                        },
                        Named.as("mongo-projection-sink"));
    }

    /**
     * The {@code ValueJoiner}: Avro in, domain out. This is the boundary where {@code adapters/}
     * stops and {@code domain/} starts (ADR-0007) - no generated Avro type is visible downstream
     * of this method.
     *
     * <p>{@code config} is {@code null} on a left-join miss: either the merchant was never
     * configured, or its config was tombstoned. Both are represented as "no name, no threshold"
     * rather than as an exception or a dropped record.
     *
     * <p>The latency measurement is the one deliberately imprecise number in this module.
     * {@code now - envelope.occurredAt} is the time from "the provider answered" to "analytics
     * processed it": broker append + replication + consumer fetch + any lag. It is a genuinely
     * useful pipeline-health metric and it is <b>not</b> payment authorization latency, which
     * would need {@code payment-requested} joined against {@code payment-status-changed} - a
     * stream-stream join across a {@code paymentId}-keyed and a {@code merchantId}-keyed topic,
     * i.e. the M13 join that finally forces a repartition topic into this application. Because it
     * reads a clock, it is also the one part of the aggregate that does not reproduce identically
     * on a replay from offset 0 (the counters do).
     */
    private static PaymentOutcome toOutcome(
            PaymentStatusChanged payment, MerchantConfigChanged config, Clock clock) {

        long occurredAtMillis =
                payment.getEnvelope() != null && payment.getEnvelope().getOccurredAt() != null
                        ? payment.getEnvelope().getOccurredAt().toEpochMilli()
                        : clock.millis();
        long latencyMillis = Math.max(0L, clock.millis() - occurredAtMillis);

        return new PaymentOutcome(
                payment.getMerchantId(),
                DECLINED.equalsIgnoreCase(payment.getStatus()),
                latencyMillis,
                config == null ? null : config.getDisplayName(),
                config == null ? null : config.getDeclineRateAlertThresholdBps());
    }
}
