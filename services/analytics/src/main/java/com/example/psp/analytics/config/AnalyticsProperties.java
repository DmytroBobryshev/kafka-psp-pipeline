package com.example.psp.analytics.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code analytics.*} from {@code application.yml} (M10). Every knob that shapes the
 * topology or its disk footprint is here rather than hard-coded, because the two things this
 * module is graded on - what internal topics appear and how much RocksDB writes - are both
 * decided by these values.
 *
 * @param kafka            topic names.
 * @param schemaRegistry   registry endpoint (environment-specific, see
 *                         {@code application-docker-compose.yml}).
 * @param streams          Kafka Streams runtime configuration.
 * @param windows          windowing and state-store retention.
 * @param authorizationJoin the M13 stream-stream join's window.
 * @param batchListener    the M13 batch listener's consumer group and batch size.
 */
@ConfigurationProperties(prefix = "analytics")
public record AnalyticsProperties(
        Kafka kafka,
        SchemaRegistry schemaRegistry,
        Streams streams,
        Windows windows,
        AuthorizationJoin authorizationJoin,
        BatchListener batchListener) {

    /**
     * @param paymentStatusChangedTopic the M10 aggregation input, and one side of the M13 join
     *                                  (re-keyed to {@code paymentId} first - see
     *                                  {@code adapters.in.kafka.AnalyticsTopology}). Keyed by
     *                                  {@code merchantId} (ADR-0003) - the single fact that keeps
     *                                  a repartition topic out of the M10 half of this
     *                                  application. Avro since M9 Phase 2.
     * @param merchantConfigChangedTopic the compacted config topic read as a
     *                                  {@code GlobalKTable}. Avro since M10.
     * @param paymentRequestedTopic     the M13 join's other input. Keyed by {@code paymentId}
     *                                  already (ADR-0003) - the side that needs NO repartition,
     *                                  which is what makes the re-keyed
     *                                  {@code payments.payment-status-changed.v1} the only side
     *                                  that pays for one. Avro since M9 Phase 1.
     */
    public record Kafka(
            String paymentStatusChangedTopic,
            String merchantConfigChangedTopic,
            String paymentRequestedTopic) {
    }

    /**
     * The M13 stream-stream join's window (see {@code adapters.in.kafka.AnalyticsTopology}'s
     * class javadoc for the full justification).
     *
     * @param window the maximum time a {@code payment-status-changed} record may arrive AFTER its
     *               matching {@code payment-requested} record and still be joined. psp-connector
     *               simulates 100ms-5s of provider latency (docs/PLAN.md's M4 brief); 5 minutes is
     *               ~60x that worst case, generous enough to survive a consumer rebalance or a
     *               slow catch-up after downtime, tight enough that the join's internal buffer
     *               stores do not grow without bound.
     * @param grace  how long after the window closes a late record is still accepted - the same
     *               30s M10 already uses for the same pipeline, and the same producer-side jitter
     *               sources (linger.ms, retry backoff on a leader election, clock skew).
     */
    public record AuthorizationJoin(java.time.Duration window, java.time.Duration grace) {
    }

    /**
     * The M13 batch listener's consumer group and the {@code max.poll.records} lever.
     *
     * @param groupId        a dedicated {@code group.id}, independent of
     *                       {@code streams.applicationId} - a plain {@code @KafkaListener} and a
     *                       Kafka Streams app are two different consumer groups on the same
     *                       topic, each with its own committed offsets and its own view of "how
     *                       far behind am I".
     * @param maxPollRecords the batch-size lever: how many records one {@code poll()} hands to
     *                       the listener in a single call, and therefore how many documents one
     *                       bulk Mongo write covers. Unlike psp-connector's M4
     *                       {@code max.poll.records=10} (kept small because each record blocks on
     *                       a slow simulated provider call), this listener does no per-record I/O
     *                       until the bulk write at the end, so a much larger batch is both safe
     *                       and the entire point.
     */
    public record BatchListener(String groupId, int maxPollRecords) {
    }

    /** @param url Confluent Schema Registry base URL; {@code mock://...} in tests. */
    public record SchemaRegistry(String url) {
    }

    /**
     * @param applicationId          THE most consequential string in the service. It is the
     *                               consumer {@code group.id}, the prefix of every internal topic
     *                               name, and the sub-directory name under {@code stateDir}.
     *                               Changing it silently starts a brand-new application with
     *                               empty state and a fresh set of changelog topics rather than
     *                               resuming - which is exactly why it carries a {@code .v1}
     *                               suffix and matches docs/diagrams/topic-map.md verbatim.
     * @param stateDir               where RocksDB writes. Explicit rather than the default
     *                               {@code ${java.io.tmpdir}/kafka-streams}, because "my
     *                               state vanished on reboot" and "my disk filled up" are both
     *                               consequences of not knowing where this points.
     * @param numStreamThreads       stream threads in this instance.
     * @param processingGuarantee    {@code at_least_once} or {@code exactly_once_v2}.
     * @param applicationServer      {@code host:port} advertised to other instances so
     *                               {@code KafkaStreams#queryMetadataForKey} can route an
     *                               interactive query to whoever owns the key.
     * @param commitInterval         how often offsets are committed AND the record cache is
     *                               flushed downstream - so it also sets the upper bound on how
     *                               often the Mongo projection is written per key.
     * @param stateStoreCacheMaxBytes record cache across all stores in this instance. Collapses
     *                               repeated updates to the same key; 0 disables it and turns
     *                               every input record into a downstream emit.
     */
    public record Streams(
            String applicationId,
            String stateDir,
            int numStreamThreads,
            String processingGuarantee,
            String applicationServer,
            Duration commitInterval,
            long stateStoreCacheMaxBytes) {
    }

    /**
     * @param size                        tumbling window size (M10 brief: 1 minute).
     * @param grace                       how long after a window's end a late record is still
     *                                    accepted into it. Also part of the windowed store's
     *                                    minimum retention.
     * @param storeRetention              how long the windowed RocksDB store (and therefore its
     *                                    changelog) keeps closed windows. Must be
     *                                    {@code >= size + grace}; everything above that minimum
     *                                    is purely "how far back can an interactive query see",
     *                                    paid for in disk.
     * @param changelogAdditionalRetention added to {@code storeRetention} to compute the
     *                                    changelog topic's {@code retention.ms}. Kafka's default
     *                                    is 24 h of slack; on a laptop that is 24 h of windowed
     *                                    changelog nobody will ever read.
     */
    public record Windows(
            Duration size,
            Duration grace,
            Duration storeRetention,
            Duration changelogAdditionalRetention) {

        public Windows {
            if (storeRetention.compareTo(size.plus(grace)) < 0) {
                throw new IllegalArgumentException(
                        "analytics.windows.store-retention ("
                                + storeRetention
                                + ") must be >= size + grace ("
                                + size.plus(grace)
                                + ") - Kafka Streams rejects a windowed store whose retention cannot"
                                + " hold a window for its whole grace period");
            }
        }
    }
}
