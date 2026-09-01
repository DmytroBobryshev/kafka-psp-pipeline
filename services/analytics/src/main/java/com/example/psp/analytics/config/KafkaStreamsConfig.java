package com.example.psp.analytics.config;

import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.common.events.avro.PaymentRequested;
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

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig(
            KafkaProperties kafkaProperties, AnalyticsProperties properties) {

        AnalyticsProperties.Streams streams = properties.streams();
        Map<String, Object> config = new HashMap<>();

        config.putAll(kafkaProperties.getProperties());

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, streams.applicationId());

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

        config.put(StreamsConfig.STATE_DIR_CONFIG, streams.stateDir());

        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streams.numStreamThreads());

        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, streams.processingGuarantee());

        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, streams.applicationServer());

        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, (int) streams.commitInterval().toMillis());
        config.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, streams.stateStoreCacheMaxBytes());

        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        config.put(
                StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG),
                String.valueOf(2));

        config.put(
                StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG,
                properties.windows().changelogAdditionalRetention().toMillis());

        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // --- RocksDB memory ------------------------------------------------------------------------
        config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, BoundedMemoryRocksDbConfigSetter.class);

        config.put(
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        // --- consumer overrides ----------------------------------------------------------------------
        config.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "earliest");
        config.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG),
                "read_committed");

        // --- producer overrides (internal topics: changelogs, and repartitions if any appear) ---------
        config.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        config.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");

        return new KafkaStreamsConfiguration(config);
    }

    @Bean
    public SpecificAvroSerde<PaymentStatusChanged> paymentStatusChangedSerde(
            AnalyticsProperties properties) {
        return avroSerde(properties.schemaRegistry().url());
    }

    @Bean
    public SpecificAvroSerde<MerchantConfigChanged> merchantConfigChangedSerde(
            AnalyticsProperties properties) {
        return avroSerde(properties.schemaRegistry().url());
    }

    @Bean
    public SpecificAvroSerde<PaymentRequested> paymentRequestedSerde(AnalyticsProperties properties) {
        return avroSerde(properties.schemaRegistry().url());
    }

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
