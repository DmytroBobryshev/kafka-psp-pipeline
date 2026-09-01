package com.example.psp.webhooknotifier.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "webhook-notifier")
public record WebhookNotifierProperties(
        Kafka kafka, Retry retry, DlqReplay dlqReplay, MerchantClient merchantClient, Mongo mongo) {

    public record Kafka(
            String paymentStatusChangedTopic,
            String refundCompletedTopic,
            String refundFailedTopic,
            String refundStatusChangedTopic,
            String deliveryRequestedTopic,
            String retry5sTopic,
            String retry1mTopic,
            String retry15mTopic,
            String dlqTopic,
            String merchantConfigChangedTopic,
            String plannerGroupId,
            String executorGroupId,
            String merchantViewGroupId,
            boolean deserializationErrorHandlingEnabled) {}

    public record Retry(long delay5sMs, long delay1mMs, long delay15mMs) {}

    public record DlqReplay(String consumerGroup, int maxBatchSize, long pollTimeoutMs) {}

    public record MerchantClient(String baseUrl, String webhookPath, long connectTimeoutMs, long readTimeoutMs) {}

    public record Mongo(long attemptLogTtlSeconds) {}
}
