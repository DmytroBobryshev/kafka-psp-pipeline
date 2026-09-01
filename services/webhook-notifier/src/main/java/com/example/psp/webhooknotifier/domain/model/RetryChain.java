package com.example.psp.webhooknotifier.domain.model;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class RetryChain {

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
