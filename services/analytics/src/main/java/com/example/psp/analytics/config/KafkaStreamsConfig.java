package com.example.psp.analytics.config;

import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

/**
 * The Kafka Streams runtime configuration (M10). Every setting the module's brief calls out is
 * here, with the reasoning attached.
 *
 * <p>{@code @EnableKafkaStreams} makes spring-kafka create a {@code StreamsBuilderFactoryBean}
 * from the {@link KafkaStreamsConfiguration} bean below (which MUST be named
 * {@code defaultKafkaStreamsConfig} - the constant is used rather than the literal), expose its
 * {@code StreamsBuilder} as a bean, and start/stop the {@code KafkaStreams} client with the
 * application context. {@code adapters.in.kafka.AnalyticsTopology} injects that builder and
 * describes the topology; nothing here knows what the topology contains.
 */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig(
            KafkaProperties kafkaProperties, AnalyticsProperties properties) {

        AnalyticsProperties.Streams streams = properties.streams();
        Map<String, Object> config = new HashMap<>();

        // --- application.id ---------------------------------------------------------------------
        // The single most consequential value in the file. It is simultaneously:
        //   * the consumer group.id for every source topic,
        //   * the prefix of every internal topic Streams creates
        //     (analytics-streams.v1-merchant-metrics-1m-changelog),
        //   * the directory name under state.dir that holds this application's RocksDB files,
        //   * the transactional.id prefix, if processing.guarantee is ever set to exactly_once_v2.
        // Changing it does not "rename" anything: it starts a brand-new application with empty
        // state, a fresh consumer group at auto.offset.reset, and a fresh set of changelog topics,
        // while the old ones sit on disk forever. Hence the explicit .v1 suffix, matching
        // docs/diagrams/topic-map.md verbatim.
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, streams.applicationId());

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

        // --- state.dir --------------------------------------------------------------------------
        // Where RocksDB writes. The default is ${java.io.tmpdir}/kafka-streams, i.e. /var/folders/...
        // on macOS - which is both hard to find and eligible for OS cleanup, so "my state
        // disappeared" and "I cannot find what is using my disk" are both symptoms of leaving it
        // at the default. Pointed at a stable, named path so the state-restore proof has something
        // it can deliberately delete. Layout underneath it:
        //   <state.dir>/<application.id>/<taskId>/rocksdb/<storeName>/
        config.put(StreamsConfig.STATE_DIR_CONFIG, streams.stateDir());

        // --- num.stream.threads -----------------------------------------------------------------
        // Threads in THIS instance. The unit of parallelism is the task, and task count is fixed
        // by the topology: one per partition of the (single) sub-topology's input, so 12 tasks for
        // payments.payment-status-changed.v1's 12 partitions. Threads only decide how many of those
        // 12 run concurrently - they do NOT change how many RocksDB instances exist (12, always) or
        // how many changelog partitions are created (12, always).
        // Raising this above the partition count creates idle threads; the ceiling is 12 here.
        // 2 is chosen for a laptop: enough to see task assignment across threads in the logs and to
        // make a rebalance observable, low enough that RocksDB compaction threads plus 12 stores do
        // not saturate a dev machine.
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streams.numStreamThreads());

        // --- processing.guarantee ---------------------------------------------------------------
        // at_least_once, deliberately, even though ledger (M7) proves this cluster supports EOS.
        //   * exactly_once_v2 would wrap each commit in a Kafka transaction covering the changelog
        //     writes and the offset commit. It would make STATE updates exactly-once.
        //   * It would NOT make the MongoDB projection exactly-once: that write is not enrolled in
        //     any Kafka transaction, exactly the boundary services/ledger/README.md's "Where Kafka
        //     EOS ends" describes. The projection therefore has to be idempotent regardless - and
        //     it is (whole-document replace keyed on merchantId|windowStart). Once the only
        //     non-Kafka side effect is idempotent, EOS buys accuracy in the state store alone.
        //   * The counters it would protect are approximate metrics whose consumer is a dashboard.
        //     Paying for transaction markers on every commit, a forced commit.interval.ms of 100 ms
        //     by default, and read_committed end-to-end latency to make a decline-rate gauge
        //     exactly right is the wrong trade at this volume.
        // Flip it here to see EOS applied to Streams rather than to a hand-rolled producer; nothing
        // else in this service needs to change.
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, streams.processingGuarantee());

        // --- application.server ------------------------------------------------------------------
        // Advertised to the other members of the group so KafkaStreams#queryMetadataForKey can tell
        // a caller which instance owns a key. Setting it costs nothing and is a precondition for
        // multi-instance interactive queries; the RPC hop that uses it is deferred (README,
        // "Compromises").
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, streams.applicationServer());

        // --- commit.interval.ms + statestore.cache.max.bytes --------------------------------------
        // These two together decide how often anything downstream of the aggregation SEES an
        // update - which here means how often MongoDB is written. The record cache holds the
        // latest value per key and forwards on eviction or commit; a commit flushes it. So a
        // merchant taking 500 payments in a minute produces ~1 emit per commit interval, not 500.
        // Setting the cache to 0 is the standard "why am I not seeing every update?" fix and turns
        // this into 500 Mongo writes - correct behaviour, and a good way to melt a laptop.
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, (int) streams.commitInterval().toMillis());
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, streams.stateStoreCacheMaxBytes());

        // --- internal topic replication -----------------------------------------------------------
        // Streams CREATES its own changelog/repartition topics, and its default replication.factor
        // is 1 - which would quietly give this application's state a durability guarantee weaker
        // than every hand-created topic in docs/diagrams/topic-map.md. Set to the cluster standard.
        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        config.put(
                StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG),
                String.valueOf(2));

        // --- windowstore.changelog.additional.retention.ms ------------------------------------------
        // The windowed store's changelog gets retention.ms = store retention + THIS. Kafka's
        // default slack is 24 h, which on a 1-minute window means the changelog holds ~1440x more
        // history than the store it backs. Trimmed hard: this is a laptop with ~36 GiB free.
        config.put(
                StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG,
                properties.windows().changelogAdditionalRetention().toMillis());

        // --- num.standby.replicas ------------------------------------------------------------------
        // 0 (the default, set explicitly because it is a disk decision, not an oversight). A standby
        // replica is a second full copy of every store on another instance, kept warm to make
        // failover fast. It doubles the on-disk state for a service whose recovery story is
        // "replay the changelog" and whose durable answers live in MongoDB anyway.
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);

        // --- serdes --------------------------------------------------------------------------------
        // Keys are plain UTF-8 strings everywhere in this system (ADR-0003), so a default key serde
        // is safe. There is deliberately NO default value serde: this topology's two source topics
        // and its state store use three different value formats (two Avro, one JSON), and a default
        // would let a node that forgot its serde pick up a silently wrong one instead of failing.
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // --- RocksDB memory ------------------------------------------------------------------------
        config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedMemoryRocksDbConfigSetter.class);

        // --- deserialization failures ---------------------------------------------------------------
        // docs/diagrams/topic-map.md, "Dead-letter topics for other consumers": analytics
        // deliberately has NO DLQ - "they log, count, and skip, and are rebuilt by resetting
        // offsets". LogAndContinue is that policy in code. It matters concretely here rather than
        // theoretically: payments.payment-status-changed.v1 still holds the pre-M9 JSON backlog
        // from M3-M8, and this application starts at earliest, so the Avro deserializer WILL hit
        // records it cannot read on its first run. The default handler (LogAndFail) would kill the
        // stream thread on the first one.
        config.put(
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        // --- consumer overrides ----------------------------------------------------------------------
        config.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "earliest");
        // Mandatory for anything downstream of the transactional ledger (topic-map.md's consumer
        // rules). It costs nothing on a topic with no transactional producer, and prevents a
        // future producer change from silently exposing aborted records to the aggregation.
        config.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG),
                "read_committed");

        // --- producer overrides (internal topics: changelogs, and repartitions if any appear) ---------
        config.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        config.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");

        return new KafkaStreamsConfiguration(config);
    }

    /**
     * Serde for {@code payments.payment-status-changed.v1}'s values (Avro since M9 Phase 2).
     *
     * <p>{@code specific.avro.reader=true} is what makes the deserializer return the generated
     * {@link PaymentStatusChanged} class rather than a {@code GenericRecord}. Without it the
     * topology still runs and then fails with a {@code ClassCastException} inside the first
     * {@code ValueJoiner} - a failure that looks like a Streams problem and is a serde problem.
     *
     * <p>{@code isKey=false}: this serde is only ever used for record values. Keys are
     * {@code Serdes.String()} (ADR-0003) and have no Schema Registry subject at all.
     */
    @Bean
    public SpecificAvroSerde<PaymentStatusChanged> paymentStatusChangedSerde(
            AnalyticsProperties properties) {
        return avroSerde(properties.schemaRegistry().url());
    }

    /**
     * Serde for {@code merchants.merchant-config-changed.v1}'s values (Avro since M10).
     *
     * <p>Handles the tombstone case for free, and it is worth being explicit about why: Confluent's
     * deserializer returns {@code null} for a {@code null} payload without consulting Schema
     * Registry at all (there is no magic byte and no schema id to look up). Kafka Streams then
     * interprets that {@code null} as a delete on the {@code GlobalKTable}. Neither this bean nor
     * the topology contains a single line of tombstone-handling code - that is the payoff of
     * expressing deletion as a null value rather than as a flag.
     */
    @Bean
    public SpecificAvroSerde<MerchantConfigChanged> merchantConfigChangedSerde(
            AnalyticsProperties properties) {
        return avroSerde(properties.schemaRegistry().url());
    }

    /**
     * The clock the topology reads to compute pipeline latency. A bean rather than
     * {@code Instant.now()} inline so {@code AnalyticsTopologyTest} can drive
     * {@code TopologyTestDriver} against a fixed instant and assert exact numbers instead of
     * ranges.
     */
    @Bean
    public java.time.Clock analyticsClock() {
        return java.time.Clock.systemUTC();
    }

    private <T extends org.apache.avro.specific.SpecificRecord> SpecificAvroSerde<T> avroSerde(
            String schemaRegistryUrl) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(
                Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true),
                false);
        return serde;
    }
}
