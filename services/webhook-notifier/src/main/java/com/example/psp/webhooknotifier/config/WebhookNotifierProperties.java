package com.example.psp.webhooknotifier.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds every {@code webhook-notifier.*} property that is not {@code simulated-merchant.*} (see
 * {@link SimulatedMerchantProperties} for that group, kept separate because it models an entirely
 * different concern - the fake external system, not this service's own behaviour).
 */
@ConfigurationProperties(prefix = "webhook-notifier")
public record WebhookNotifierProperties(
        Kafka kafka, Retry retry, DlqReplay dlqReplay, MerchantClient merchantClient, Mongo mongo) {

    /**
     * @param paymentStatusChangedTopic inbound topic for the planner.
     * @param refundCompletedTopic      M19: second inbound planner topic, {@code refunds.refund-completed.v1}.
     * @param refundFailedTopic         M19: third inbound planner topic, {@code refunds.refund-failed.v1}.
     * @param refundStatusChangedTopic  M24: fourth inbound planner topic, {@code refunds.refund-status-changed.v1} -
     *                          {@code adapters.in.kafka.RefundExpiredListener} plans a delivery
     *                          only for its {@code EXPIRED} records (see that class's javadoc).
     * @param deliveryRequestedTopic    base topic of the delivery/retry chain.
     * @param retry5sTopic              tier 1.
     * @param retry1mTopic              tier 2.
     * @param retry15mTopic             tier 3 (last tier before the DLQ).
     * @param dlqTopic                  terminal topic.
     * @param merchantConfigChangedTopic the compacted merchant-config topic
     *                          ({@code merchants.merchant-config-changed.v1}) this service
     *                          projects into {@code merchant_webhooks} for delivery-time URL
     *                          resolution - see {@code adapters.in.kafka.MerchantConfigChangedListener}.
     * @param plannerGroupId            {@code webhook-notifier.planner.v1} (docs/diagrams/topic-map.md).
     * @param executorGroupId           {@code webhook-notifier.executor.v1} - subscribed to the
     *                                  base topic plus all three retry tiers.
     * @param merchantViewGroupId       {@code webhook-notifier.merchant-view.v1} - independent of
     *                                  both groups above, reading {@code merchantConfigChangedTopic}.
     * @param deserializationErrorHandlingEnabled THE M8 poison-pill flag. {@code true} (default)
     *                          wraps every consumer factory's deserializer in
     *                          {@code ErrorHandlingDeserializer} (ADR-0006 category C). Set to
     *                          {@code false} - {@code --webhook-notifier.kafka.deserialization-error-handling-enabled=false} -
     *                          to reproduce the FAILURE this exists to fix: a record whose bytes
     *                          cannot be deserialized throws out of {@code poll()} itself, and the
     *                          container retries the SAME offset forever, never advancing,
     *                          consuming 100% CPU on one partition while every other message
     *                          behind it queues up unprocessed. See
     *                          {@code config.KafkaConsumerConfig} and the README's "Poison pill
     *                          proof" placeholder.
     */
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

    /**
     * Retry-tier delays. Default to ADR-0006's real values (5s/1m/15m -> ~16 minutes to DLQ), but
     * every field is override-able from the command line so an experiment can shrink the whole
     * chain to a few seconds without touching the topic topology - e.g.
     * {@code --webhook-notifier.retry.delay-5s-ms=2000 --webhook-notifier.retry.delay-1m-ms=4000
     * --webhook-notifier.retry.delay-15m-ms=6000} - see the README's "Retry chain proof"
     * placeholder.
     */
    public record Retry(long delay5sMs, long delay1mMs, long delay15mMs) {}

    /**
     * @param consumerGroup  dedicated {@code group.id} for {@code adapters.out.kafka.KafkaDlqReader}
     *                       - separate from {@code webhook-notifier.executor.v1} so replay reads
     *                       never interact with the executor's own DLQ-producing consumption of
     *                       every OTHER topic.
     * @param maxBatchSize   the hard ceiling on one replay call, regardless of what the REST
     *                       caller requests (the "guard" in M8 requirement #7). Also configures the
     *                       replay consumer factory's {@code max.poll.records}, so one
     *                       {@code poll()} can never return more than this many records to begin
     *                       with.
     * @param pollTimeoutMs  how long one replay call waits for records before returning
     *                       (fewer-than-requested or zero is a normal, non-error outcome - it just
     *                       means the DLQ currently holds fewer unreplayed records than asked for).
     */
    public record DlqReplay(String consumerGroup, int maxBatchSize, long pollTimeoutMs) {}

    /**
     * @param baseUrl          where {@code adapters.out.http.RestClientMerchantWebhookClient}
     *                         sends the callback. Defaults to this same service
     *                         ({@code http://localhost:8088}, the in-process simulated merchant);
     *                         point it at a real endpoint with zero code change.
     * @param webhookPath      path template, {@code {merchantId}} substituted.
     * @param connectTimeoutMs TCP connect timeout.
     * @param readTimeoutMs    response read timeout - MUST be shorter than
     *                         {@code webhook-notifier.simulated-merchant.timeout-delay-ms} for the
     *                         TIMEOUT simulation to actually produce a client-side timeout.
     */
    public record MerchantClient(String baseUrl, String webhookPath, long connectTimeoutMs, long readTimeoutMs) {}

    /**
     * @param attemptLogTtlSeconds TTL for {@code delivery_attempts} documents (M8 requirement #6).
     *                             Default 2,592,000s (30 days), matching the DLQ topic's own
     *                             retention (ADR-0006: "long enough to notice and fix") - by the
     *                             time an attempt document expires, its DLQ record (if any) has
     *                             already expired too.
     */
    public record Mongo(long attemptLogTtlSeconds) {}
}
