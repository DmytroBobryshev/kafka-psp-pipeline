package com.example.psp.webhooknotifier.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.webhooknotifier.domain.model.DeliveryAttempt;
import com.example.psp.webhooknotifier.domain.model.DeliveryOutcome;
import com.example.psp.webhooknotifier.domain.model.DeliveryResult;
import com.example.psp.webhooknotifier.domain.model.RecordCoordinates;
import com.example.psp.webhooknotifier.domain.model.RetryChain;
import com.example.psp.webhooknotifier.domain.model.RetryEnvelope;
import com.example.psp.webhooknotifier.domain.model.WebhookDelivery;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import com.example.psp.webhooknotifier.domain.port.DeliveryAttemptLogRepository;
import com.example.psp.webhooknotifier.domain.port.MerchantWebhookClient;
import com.example.psp.webhooknotifier.domain.port.WebhookDeliveryPublisher;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ExecuteWebhookDeliveryUseCaseTest {

    private static final RetryChain CHAIN =
            new RetryChain(
                    "base",
                    List.of(
                            new RetryChain.Tier("retry5s", Duration.ofSeconds(5)),
                            new RetryChain.Tier("retry1m", Duration.ofMinutes(1)),
                            new RetryChain.Tier("retry15m", Duration.ofMinutes(15))),
                    "dlq");

    @Test
    void successIsLoggedAndNothingIsPublished() {
        FakeMerchantClient client = new FakeMerchantClient(DeliveryResult.success(200));
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ExecuteWebhookDeliveryUseCase useCase = new ExecuteWebhookDeliveryUseCase(client, attemptLog, publisher, CHAIN);

        useCase.execute(command(), coordinates("base"), RetryEnvelope.initial()).join();

        assertThat(attemptLog.recorded).hasSize(1);
        assertThat(attemptLog.recorded.get(0).outcome()).isEqualTo(DeliveryOutcome.SUCCESS);
        assertThat(attemptLog.recorded.get(0).attemptNumber()).isEqualTo(1);
        assertThat(publisher.published).isEmpty();
    }

    @Test
    void nonRetryableFailureIsLoggedAndRoutedStraightToDlq() {
        FakeMerchantClient client = new FakeMerchantClient(DeliveryResult.nonRetryable(400, "bad payload"));
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ExecuteWebhookDeliveryUseCase useCase = new ExecuteWebhookDeliveryUseCase(client, attemptLog, publisher, CHAIN);

        useCase.execute(command(), coordinates("base"), RetryEnvelope.initial()).join();

        assertThat(attemptLog.recorded).hasSize(1);
        assertThat(attemptLog.recorded.get(0).outcome()).isEqualTo(DeliveryOutcome.NON_RETRYABLE_FAILURE);

        // Never traverses the retry chain - straight to the DLQ, published immediately (not delayed).
        assertThat(publisher.published).hasSize(1);
        Published published = publisher.published.get(0);
        assertThat(published.topic()).isEqualTo("dlq");
        assertThat(published.delayed()).isFalse();
        assertThat(published.envelope().originalTopic()).isEqualTo("base");
    }

    @Test
    void retryableFailureOnBaseTopicSchedulesAHopToTheFirstTier() {
        FakeMerchantClient client = new FakeMerchantClient(DeliveryResult.retryable(503, "merchant unavailable"));
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ExecuteWebhookDeliveryUseCase useCase = new ExecuteWebhookDeliveryUseCase(client, attemptLog, publisher, CHAIN);

        useCase.execute(command(), coordinates("base"), RetryEnvelope.initial()).join();

        assertThat(attemptLog.recorded.get(0).outcome()).isEqualTo(DeliveryOutcome.RETRYABLE_FAILURE);

        assertThat(publisher.published).hasSize(1);
        Published published = publisher.published.get(0);
        assertThat(published.topic()).isEqualTo("retry5s");
        assertThat(published.delayed()).isTrue();
        assertThat(published.delay()).isEqualTo(Duration.ofSeconds(5));
        // Attempt count incremented for the NEXT hop.
        assertThat(published.envelope().attemptCount()).isEqualTo(2);
        // original-* stamped from THIS record's own coordinates - this was the first failure.
        assertThat(published.envelope().originalTopic()).isEqualTo("base");
        assertThat(published.envelope().originalPartition()).isEqualTo(0);
    }

    @Test
    void retryableFailureOnALaterTierPreservesTheOriginalOriginalCoordinates() {
        FakeMerchantClient client = new FakeMerchantClient(DeliveryResult.retryable(503, "still unavailable"));
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ExecuteWebhookDeliveryUseCase useCase = new ExecuteWebhookDeliveryUseCase(client, attemptLog, publisher, CHAIN);

        // Simulate arriving on retry5s already carrying original-* from the very first failure on "base".
        RetryEnvelope incoming =
                new RetryEnvelope(2, "base", 0, 41L, Instant.parse("2026-01-01T00:00:00Z"), "merchant-5xx-or-timeout", "first failure", null, null);

        useCase.execute(command(), coordinates("retry5s"), incoming).join();

        Published published = publisher.published.get(0);
        assertThat(published.topic()).isEqualTo("retry1m");
        // Original coordinates are UNCHANGED - "original" means the very first attempt, not this hop.
        assertThat(published.envelope().originalTopic()).isEqualTo("base");
        assertThat(published.envelope().originalOffset()).isEqualTo(41L);
        assertThat(published.envelope().attemptCount()).isEqualTo(3);
    }

    @Test
    void retryableFailureOnTheLastTierIsExhaustedAndGoesToDlq() {
        FakeMerchantClient client = new FakeMerchantClient(DeliveryResult.retryable(503, "still unavailable"));
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ExecuteWebhookDeliveryUseCase useCase = new ExecuteWebhookDeliveryUseCase(client, attemptLog, publisher, CHAIN);

        RetryEnvelope incoming =
                new RetryEnvelope(4, "base", 0, 7L, Instant.parse("2026-01-01T00:00:00Z"), "merchant-5xx-or-timeout", "third failure", null, null);

        useCase.execute(command(), coordinates("retry15m"), incoming).join();

        Published published = publisher.published.get(0);
        assertThat(published.topic()).isEqualTo("dlq");
        assertThat(published.delayed()).isFalse();
    }

    private static WebhookDeliveryCommand command() {
        return new WebhookDeliveryCommand(
                UUID.randomUUID(),
                "merchant-1",
                BigDecimal.TEN,
                "EUR",
                "SUCCEEDED",
                null,
                UUID.randomUUID(),
                "trace-1",
                "corr-1",
                "PAYMENT_STATUS_CHANGED",
                null);
    }

    private static RecordCoordinates coordinates(String topic) {
        return new RecordCoordinates(topic, 0, 99L, Instant.parse("2026-01-01T00:00:00Z"));
    }

    private record Published(
            String topic, WebhookDeliveryCommand command, RetryEnvelope envelope, boolean delayed, Duration delay) {}

    private static final class FakeMerchantClient implements MerchantWebhookClient {
        private final DeliveryResult result;

        private FakeMerchantClient(DeliveryResult result) {
            this.result = result;
        }

        @Override
        public DeliveryResult deliver(WebhookDeliveryCommand command) {
            return result;
        }
    }

    private static final class RecordingAttemptLog implements DeliveryAttemptLogRepository {
        private final List<DeliveryAttempt> recorded = new ArrayList<>();

        @Override
        public void record(DeliveryAttempt attempt) {
            recorded.add(attempt);
        }

        @Override
        public List<WebhookDelivery> search(UUID paymentId, UUID refundId, String merchantId, int limit) {
            throw new UnsupportedOperationException(
                    "search() is not part of the execute-webhook-delivery use case under test");
        }
    }

    private static final class RecordingPublisher implements WebhookDeliveryPublisher {
        private final List<Published> published = new ArrayList<>();

        @Override
        public CompletableFuture<Void> publishNow(String topic, WebhookDeliveryCommand command, RetryEnvelope envelope) {
            published.add(new Published(topic, command, envelope, false, null));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> publishDelayed(
                String topic, WebhookDeliveryCommand command, RetryEnvelope envelope, Duration delay) {
            published.add(new Published(topic, command, envelope, true, delay));
            return CompletableFuture.completedFuture(null);
        }
    }
}
