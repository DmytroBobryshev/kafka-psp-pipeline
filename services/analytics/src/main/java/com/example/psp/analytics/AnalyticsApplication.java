package com.example.psp.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * M10 entry point: the Kafka Streams service.
 *
 * <p>Reads the compacted {@code merchants.merchant-config-changed.v1} into a
 * {@code GlobalKTable}, left-joins it against the Avro {@code payments.payment-status-changed.v1}
 * stream, aggregates per-merchant volume / decline rate / average pipeline latency into 1-minute
 * tumbling windows with a 30 s grace period, serves the live window state over REST as
 * interactive queries, and projects each emitted window into MongoDB so results outlive the state
 * store.
 *
 * <p>No {@code @EnableKafkaStreams} here - it is on {@code config.KafkaStreamsConfig}, next to the
 * {@code KafkaStreamsConfiguration} bean it needs, so every Streams setting and the annotation
 * that activates them are in one file. See {@code README.md}.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class AnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }
}
