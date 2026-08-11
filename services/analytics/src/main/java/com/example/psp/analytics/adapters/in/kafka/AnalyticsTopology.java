package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.analytics.application.ProjectAuthorizationLatencyUseCase;
import com.example.psp.analytics.application.ProjectWindowMetricsUseCase;
import com.example.psp.analytics.config.AnalyticsProperties;
import com.example.psp.analytics.config.StreamsStores;
import com.example.psp.analytics.domain.model.AuthorizationLatency;
import com.example.psp.analytics.domain.model.MerchantWindowMetrics;
import com.example.psp.analytics.domain.model.PaymentOutcome;
import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import java.time.Clock;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.StreamJoined;
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
 *
 * <h2>5. The M13 join: a genuine stream-stream join, and why it needs a repartition</h2>
 *
 * <p>Everything above is one sub-topology with zero shuffles, because the {@code GlobalKTable}
 * join is never co-partitioning-constrained and {@code payments.payment-status-changed.v1}
 * already arrives keyed by the M10 aggregation's grouping key. M13 adds a second, independent
 * join that has neither property: {@code payments.payment-requested.v1} x
 * {@code payments.payment-status-changed.v1}, to compute genuine <b>authorization latency</b> -
 * {@code decidedAt - requestedAt} for one payment - which this module's M10 "Compromises" section
 * explicitly says {@code avgPipelineLatencyMillis} is not.
 *
 * <p><b>Why this needs co-partitioning, in the arithmetic sense a {@code GlobalKTable} cannot
 * sidestep.</b> A {@code KStream x KStream} windowed join is stateful <i>on both sides</i>: each
 * task buffers a local window of records from BOTH input streams (two RocksDB stores, each with
 * its own changelog - see below) and matches them by key within the task. For that matching to
 * ever succeed, the same key must be guaranteed to land in the same task on both streams - i.e.
 * the two streams must be co-partitioned (same partition count, same partitioner, same key).
 * {@code payments.payment-requested.v1} is keyed by {@code paymentId} (ADR-0003); {@code
 * payments.payment-status-changed.v1} is keyed by {@code merchantId}, deliberately, so that all
 * of one merchant's status changes stay ordered for the ledger's single-writer-per-balance
 * invariant (M7). Those are two different keys - not just two different partition counts, which a
 * {@code GlobalKTable} join can shrug off by not partitioning its table at all. A stream-stream
 * join has no "table" side to fully replicate; <b>both</b> sides are streams, both are buffered
 * per task, and a {@code GlobalKTable}-style escape hatch does not exist for this join shape at
 * all - there is no global-stream construct in the Kafka Streams DSL, because buffering every
 * task's full window of every partition on every instance would multiply the payment path's
 * highest-volume topic by the instance count, the exact catastrophe the M10 GlobalKTable section
 * warns a `GlobalKTable`'s source must never approach.
 *
 * <p>So one side has to be re-keyed to {@code paymentId} before the join can run at all. Re-keying
 * a {@code KStream} ({@code selectKey}) sets Kafka Streams' "repartition required" flag rather
 * than moving any data; the next stateful operation that needs the new partitioning - here, the
 * join itself - is what actually materializes the repartition topic. Its name comes from the
 * join's OWN name ({@code StreamJoined.withName("authorization-latency-join")} below), not from
 * the {@code selectKey}'s - Streams names an auto-repartitioned join input
 * {@code <joinName>-right-repartition} (the re-keyed side is always "right" here, since it is the
 * argument to {@code requested.join(statusByPaymentId, ...)}), confirmed on the live cluster as
 * {@code analytics-streams.v1-authorization-latency-join-right-repartition}: 12 partitions, to
 * match {@code payments.payment-requested.v1}'s own 12, written and immediately re-read through
 * the broker. {@code payments.payment-requested.v1} needs no such treatment - already keyed by
 * {@code paymentId}, it is the side this join reads unchanged, which is exactly why re-keying the
 * OTHER side is the only way to make the pair co-partitioned rather than re-keying both.
 *
 * <p><b>What the repartition costs, honestly:</b> an extra network round trip through the broker
 * for every status-changed record (produced to the repartition topic by one task, fetched back by
 * whichever task now owns that {@code paymentId}'s partition - a real hop even when, by
 * coincidence, source and destination task are the same); extra storage (the repartition topic
 * holds a full copy of every re-keyed record, {@code cleanup.policy=delete} with no long retention
 * since it is a pure in-flight relay, not state); and extra latency (a produce-then-refetch adds
 * at least one broker round trip to every record's path through this join, before the join's own
 * window buffering is even reached). None of that exists on the M10 path, which is the whole
 * point of contrasting the two: M10 proves a topology CAN avoid a shuffle when the source is
 * already correctly keyed; M13 is what the same topology looks like when it genuinely is not.
 *
 * <p><b>The join window.</b> {@code JoinWindows.ofTimeDifferenceAndGrace(window, grace)}, with
 * {@code .before(Duration.ZERO)}: a {@code payment-status-changed} record for a given {@code
 * paymentId} joins its {@code payment-requested} record only if it is timestamped at or after the
 * request (never before - a status decided "before" its own request is clock skew, not a real
 * negative latency) and within {@code window} after it. See {@code
 * AnalyticsProperties.AuthorizationJoin} for the exact values and their justification
 * (psp-connector's simulated 100ms-5s provider latency vs. the chosen window's margin).
 *
 * <p><b>What happens to a record outside the window - the important part.</b> An unjoined payment
 * is <i>not</i> a lost payment. {@code payments.payment-requested.v1} and {@code
 * payments.payment-status-changed.v1} are untouched by this join - it only reads them, and every
 * other consumer (ledger, webhook-notifier, this same application's M10 aggregation, the DLQ
 * replay APIs) sees every record on both topics exactly as before, for the topics' full 7-day
 * retention (docs/diagrams/topic-map.md). What is missing is narrower: this ONE derived,
 * analytics-only view - one authorization-latency measurement - for that one payment. Two ways a
 * record can miss the window: the match arrives late (status decided more than {@code window}
 * after the request - Streams' own {@code late-record-drop} metric counts it, exactly like M10's
 * grace-period drops, and it emits nothing, silently by design) or never arrives at all within
 * the join's internal buffer retention (a payment that is still pending, or whose status event
 * was lost upstream of this join - genuinely rare given ADR-0006's retry/DLQ policy on
 * psp-connector's consumption of {@code payments.payment-requested.v1}). Either way, this join
 * uses a plain (inner) {@code join}, not {@code leftJoin}/{@code outerJoin}: a "latency" with no
 * decision timestamp is not a smaller version of the answer, it is not an answer, so there is
 * nothing useful an outer join's null-populated result would add here - unlike M10's {@code
 * leftJoin}, where a payment with no merchant config is still a real, countable payment.
 *
 * <p><b>Internal topics this join adds, beyond the repartition topic.</b> A windowed
 * {@code KStream x KStream} join buffers both sides in RocksDB for the duration of the window, and
 * both buffers are logged stores - Streams creates TWO more changelogs. {@code
 * StreamJoined.withName(...)} alone does NOT name them - it names the processor nodes and the
 * repartition topic only; the join stores are a separate knob,
 * {@code StreamJoined.withStoreName(...)}, and skipping it leaves Streams to fall back to
 * auto-numbered names like {@code KSTREAM-JOINTHIS-0000000014-store} - exactly the kind of
 * unnamed, build-order-fragile internal topic the "every node is named" point above warns about
 * for M10. Both are set here, so the confirmed names on the live cluster are {@code
 * analytics-streams.v1-authorization-latency-join-this-join-store-changelog} (the
 * payment-requested side) and {@code ...-other-join-store-changelog} (the re-keyed
 * payment-status-changed side). That is the concrete answer to "what did Streams create now": one
 * repartition topic plus two join-buffer changelogs, on top of M10's one aggregation changelog -
 * contrast with M10's single-changelog, zero-repartition topology, which is the entire point of
 * building this join.
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
            io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde<PaymentRequested>
                    paymentRequestedSerde,
            ProjectWindowMetricsUseCase projectWindowMetricsUseCase,
            ProjectAuthorizationLatencyUseCase projectAuthorizationLatencyUseCase,
            Clock clock) {

        define(
                streamsBuilder,
                properties,
                paymentStatusChangedSerde,
                merchantConfigChangedSerde,
                paymentRequestedSerde,
                projectWindowMetricsUseCase,
                projectAuthorizationLatencyUseCase,
                clock);

        log.info(
                "Topology defined: window={} grace={} storeRetention={} authJoinWindow={} "
                        + "authJoinGrace={} stores=[{}, {}]",
                properties.windows().size(),
                properties.windows().grace(),
                properties.windows().storeRetention(),
                properties.authorizationJoin().window(),
                properties.authorizationJoin().grace(),
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
            Serde<PaymentRequested> paymentRequestedSerde,
            ProjectWindowMetricsUseCase projectWindowMetricsUseCase,
            ProjectAuthorizationLatencyUseCase projectAuthorizationLatencyUseCase,
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

        // ---- M13: the stream-stream join - see the class javadoc's section 5 for the full ------
        // ---- co-partitioning, window and internal-topics reasoning. ----------------------------

        // Source 3: payments.payment-requested.v1, keyed by paymentId already (ADR-0003). This
        // side needs NO repartition - it is the side the other one has to match, not the side
        // that moves.
        KStream<String, PaymentRequested> requested =
                builder.stream(
                        properties.kafka().paymentRequestedTopic(),
                        Consumed.with(Serdes.String(), paymentRequestedSerde)
                                .withTimestampExtractor(new EnvelopeEventTimeExtractor())
                                .withName("payment-requested-source"));

        // Re-key the ALREADY-BUILT `payments` KStream (M10's own source, reused rather than a
        // second builder.stream(paymentStatusChangedTopic, ...) call - Kafka Streams rejects
        // registering the same input topic as a source twice in one topology with
        // "Topic ... has already been registered by another source", so branching one KStream
        // into two independent downstream chains is not a style choice here, it is the only
        // legal way to read this topic from two places in the same application). selectKey sets
        // Kafka Streams' "repartition required" flag on this branch only - M10's groupByKey()
        // above is unaffected, because Kafka Streams tracks that flag per-KStream, not per-topic.
        KStream<String, PaymentStatusChanged> statusByPaymentId =
                payments.selectKey(
                        (merchantId, status) -> status.getPaymentId(),
                        Named.as("rekey-status-changed-by-payment-id"));

        // The join itself is what actually materializes the repartition topic (Streams defers
        // creating it until an operation needs the new partitioning), plus the two join-buffer
        // changelogs named below. Plain (inner) join, not leftJoin/outerJoin - see the class
        // javadoc for why an unmatched half-record is not a useful output here.
        KStream<String, AuthorizationLatency> authorizationLatency =
                requested.join(
                        statusByPaymentId,
                        (request, status) -> toAuthorizationLatency(request, status),
                        JoinWindows.ofTimeDifferenceAndGrace(
                                        properties.authorizationJoin().window(),
                                        properties.authorizationJoin().grace())
                                // Tighten the default symmetric window: the OTHER record
                                // (status-changed) may not be timestamped before THIS record
                                // (requested) at all - see the class javadoc's "join window"
                                // paragraph.
                                .before(java.time.Duration.ZERO),
                        StreamJoined.<String, PaymentRequested, PaymentStatusChanged>with(
                                        Serdes.String(), paymentRequestedSerde, paymentStatusChangedSerde)
                                .withName("authorization-latency-join")
                                // withName() alone names the processor nodes and the repartition
                                // topic; the two join-buffer STORES (and therefore their
                                // changelogs) are a separate knob and fall back to Streams'
                                // auto-numbered KSTREAM-JOINTHIS-<n>-store / KSTREAM-JOINOTHER-
                                // <n>-store if left unset - exactly the kind of unnamed,
                                // build-order-fragile internal topic the M10 class javadoc's
                                // "every node is named" point warns about. Named explicitly for
                                // the same reason M10 names every processor.
                                .withStoreName("authorization-latency-join"));

        authorizationLatency.foreach(
                (paymentId, latency) -> projectAuthorizationLatencyUseCase.project(latency),
                Named.as("authorization-latency-projection-sink"));
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

    /**
     * The M13 join's {@code ValueJoiner}: Avro in (both sides), domain out - same ADR-0007
     * boundary discipline as {@link #toOutcome}. {@code envelope.occurredAt} is read directly
     * from each side's own Avro record, not from Kafka Streams' record timestamp (which the
     * {@link EnvelopeEventTimeExtractor} sets for windowing purposes only) - see {@link
     * AuthorizationLatency#of} for why the two are the same value here but are computed
     * independently regardless.
     */
    private static AuthorizationLatency toAuthorizationLatency(
            PaymentRequested request, PaymentStatusChanged status) {
        return AuthorizationLatency.of(
                request.getPaymentId(),
                status.getMerchantId(),
                status.getProviderReference(),
                status.getStatus(),
                request.getEnvelope().getOccurredAt(),
                status.getEnvelope().getOccurredAt());
    }
}
