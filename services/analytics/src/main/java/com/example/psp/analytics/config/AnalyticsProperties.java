package com.example.psp.analytics.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "analytics")
public record AnalyticsProperties(
        Kafka kafka,
        SchemaRegistry schemaRegistry,
        Streams streams,
        Windows windows,
        AuthorizationJoin authorizationJoin,
        BatchListener batchListener) {

    public record Kafka(
            String paymentStatusChangedTopic,
            String merchantConfigChangedTopic,
            String paymentRequestedTopic) {
    }

    public record AuthorizationJoin(java.time.Duration window, java.time.Duration grace) {
    }

    public record BatchListener(String groupId, int maxPollRecords) {
    }

    public record SchemaRegistry(String url) {
    }

    public record Streams(
            String applicationId,
            String stateDir,
            int numStreamThreads,
            String processingGuarantee,
            String applicationServer,
            Duration commitInterval,
            long stateStoreCacheMaxBytes) {
    }

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
