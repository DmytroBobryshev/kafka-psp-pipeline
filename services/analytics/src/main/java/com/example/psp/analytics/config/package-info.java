/**
 * Spring configuration, bean wiring, and profiles (ADR-0007).
 *
 * <p>Everything Kafka Streams needs to exist before a topology can be described lives here:
 * {@link com.example.psp.analytics.config.KafkaStreamsConfig} (the {@code StreamsConfig} map, the
 * Avro Serdes, {@code @EnableKafkaStreams}),
 * {@link com.example.psp.analytics.config.AnalyticsProperties} (every tunable),
 * {@link com.example.psp.analytics.config.StreamsStores} (the store names, shared between the
 * topology that creates them and the adapter that queries them), and
 * {@link com.example.psp.analytics.config.BoundedMemoryRocksDbConfigSetter} (RocksDB sizing,
 * reachable only through {@code rocksdb.config.setter}).
 *
 * <p>The topology itself is deliberately NOT here - it is an inbound adapter
 * ({@code adapters.in.kafka.AnalyticsTopology}), because describing what the service consumes and
 * how it maps onto the domain is adapter work, not configuration.
 */
package com.example.psp.analytics.config;
