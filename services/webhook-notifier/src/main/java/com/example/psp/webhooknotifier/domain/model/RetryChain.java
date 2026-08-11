package com.example.psp.webhooknotifier.domain.model;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The topology of the non-blocking retry chain (ADR-0006, M8): a pure, side-effect-free
 * description of "given the topic a delivery just failed on, what topic does it go to next, and
 * after how long - or has it exhausted the chain and must go straight to the DLQ."
 *
 * <pre>
 *   webhooks.webhook-delivery-requested.v1
 *     --retryable--> .retry.5s --retryable--> .retry.1m --retryable--> .retry.15m --retryable--> .dlq
 *     --non-retryable, any tier---------------------------------------------------------------> .dlq
 * </pre>
 *
 * <p>Built once at startup from {@code webhook-notifier.kafka.*}/{@code webhook-notifier.retry.*}
 * ({@code config.RetryChainConfig}) and injected as a plain, immutable, framework-free object -
 * {@code application.ExecuteWebhookDeliveryUseCase} depends on this type, not on
 * {@code config.WebhookNotifierProperties}, keeping the use case ignorant of where the topology
 * came from (ADR-0007). Trivially unit-testable without Spring or Kafka: see
 * {@code domain.model.RetryChainTest}.
 */
public final class RetryChain {

    /** One hop of the chain: a topic name and how long a delivery must wait before landing there. */
    public record Tier(String topic, Duration delay) {
        public Tier {
            Objects.requireNonNull(topic, "topic must not be null");
            Objects.requireNonNull(delay, "delay must not be null");
        }
    }

    private final String baseTopic;
    private final List<Tier> tiers;
    private final String dlqTopic;

    public RetryChain(String baseTopic, List<Tier> tiers, String dlqTopic) {
        this.baseTopic = Objects.requireNonNull(baseTopic, "baseTopic must not be null");
        this.tiers = List.copyOf(Objects.requireNonNull(tiers, "tiers must not be null"));
        this.dlqTopic = Objects.requireNonNull(dlqTopic, "dlqTopic must not be null");
        if (this.tiers.isEmpty()) {
            throw new IllegalArgumentException("retry chain must have at least one tier");
        }
    }

    public String baseTopic() {
        return baseTopic;
    }

    public String dlqTopic() {
        return dlqTopic;
    }

    /**
     * The tier a retryable failure on {@code currentTopic} should hop to next, or
     * {@link Optional#empty()} if {@code currentTopic} is already the last retry tier - the
     * signal to publish to {@link #dlqTopic()} instead.
     *
     * @throws IllegalArgumentException if {@code currentTopic} is not the base topic or one of
     *     this chain's own tiers - a defensive check: it means a record arrived on this
     *     consumer group carrying a topic this chain does not know about, which is a
     *     configuration bug, not a runtime condition to route around.
     */
    public Optional<Tier> nextTierAfter(String currentTopic) {
        if (currentTopic.equals(baseTopic)) {
            return Optional.of(tiers.get(0));
        }
        for (int i = 0; i < tiers.size(); i++) {
            if (tiers.get(i).topic().equals(currentTopic)) {
                return i + 1 < tiers.size() ? Optional.of(tiers.get(i + 1)) : Optional.empty();
            }
        }
        throw new IllegalArgumentException(
                "topic '"
                        + currentTopic
                        + "' is not part of this retry chain (base="
                        + baseTopic
                        + ", tiers="
                        + tiers
                        + ")");
    }
}
