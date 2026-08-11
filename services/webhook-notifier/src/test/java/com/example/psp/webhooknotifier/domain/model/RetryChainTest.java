package com.example.psp.webhooknotifier.domain.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Plain JUnit over {@link RetryChain} - no Spring, no Kafka, nothing but the topology logic
 * itself. Proves the chain's routing is correct independent of anything that publishes to it.
 */
class RetryChainTest {

    private static final RetryChain CHAIN =
            new RetryChain(
                    "base",
                    List.of(
                            new RetryChain.Tier("retry5s", Duration.ofSeconds(5)),
                            new RetryChain.Tier("retry1m", Duration.ofMinutes(1)),
                            new RetryChain.Tier("retry15m", Duration.ofMinutes(15))),
                    "dlq");

    @Test
    void baseTopicHopsToTheFirstTier() {
        assertThat(CHAIN.nextTierAfter("base"))
                .contains(new RetryChain.Tier("retry5s", Duration.ofSeconds(5)));
    }

    @Test
    void firstTierHopsToTheSecondTier() {
        assertThat(CHAIN.nextTierAfter("retry5s"))
                .contains(new RetryChain.Tier("retry1m", Duration.ofMinutes(1)));
    }

    @Test
    void secondTierHopsToTheThirdTier() {
        assertThat(CHAIN.nextTierAfter("retry1m"))
                .contains(new RetryChain.Tier("retry15m", Duration.ofMinutes(15)));
    }

    @Test
    void lastTierIsExhausted() {
        // Empty means "go to the DLQ instead" - see application.ExecuteWebhookDeliveryUseCase.
        assertThat(CHAIN.nextTierAfter("retry15m")).isEmpty();
    }

    @Test
    void unknownTopicIsRejected() {
        assertThatThrownBy(() -> CHAIN.nextTierAfter("some-unrelated-topic"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void baseAndDlqTopicsAreExposed() {
        assertThat(CHAIN.baseTopic()).isEqualTo("base");
        assertThat(CHAIN.dlqTopic()).isEqualTo("dlq");
    }
}
