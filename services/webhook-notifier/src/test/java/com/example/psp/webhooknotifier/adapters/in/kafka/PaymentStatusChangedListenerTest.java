package com.example.psp.webhooknotifier.adapters.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.common.events.avro.EventEnvelope;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.webhooknotifier.application.PlanWebhookDeliveryUseCase;
import com.example.psp.webhooknotifier.domain.model.RetryChain;
import com.example.psp.webhooknotifier.domain.model.RetryEnvelope;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import com.example.psp.webhooknotifier.domain.port.WebhookDeliveryPublisher;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class PaymentStatusChangedListenerTest {

    @Test
    void pendingStatusIsSkippedAndAcknowledgedWithoutPlanningADelivery() {
        assertSkippedAndAcknowledged("PENDING");
    }

    @Test
    void ipnReceivedStatusIsSkippedAndAcknowledgedWithoutPlanningADelivery() {
        assertSkippedAndAcknowledged("IPN_RECEIVED");
    }

    @Test
    void verifiedStatusIsSkippedAndAcknowledgedWithoutPlanningADelivery() {
        assertSkippedAndAcknowledged("VERIFIED");
    }

    @Test
    void expiredStatusIsPlannedAndAcknowledgedJustLikeSucceededOrDeclined() {
        RecordingPublisher publisher = new RecordingPublisher();
        RetryChain chain =
                new RetryChain("base", List.of(new RetryChain.Tier("retry5s", Duration.ofSeconds(5))), "dlq");
        PlanWebhookDeliveryUseCase useCase = new PlanWebhookDeliveryUseCase(publisher, chain);
        PaymentStatusChangedListener listener =
                new PaymentStatusChangedListener(useCase, new PaymentStatusChangedMapperImpl());
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        listener.onMessage(eventWithStatus("EXPIRED"), ack);

        assertThat(publisher.published).hasSize(1);
        assertThat(publisher.published.get(0).eventType()).isEqualTo("PAYMENT_STATUS_CHANGED");
        assertThat(publisher.published.get(0).status()).isEqualTo("EXPIRED");
        assertThat(ack.acknowledged).isTrue();
    }

    private static void assertSkippedAndAcknowledged(String status) {
        RecordingPublisher publisher = new RecordingPublisher();
        RetryChain chain =
                new RetryChain("base", List.of(new RetryChain.Tier("retry5s", Duration.ofSeconds(5))), "dlq");
        PlanWebhookDeliveryUseCase useCase = new PlanWebhookDeliveryUseCase(publisher, chain);
        PaymentStatusChangedListener listener =
                new PaymentStatusChangedListener(useCase, new PaymentStatusChangedMapperImpl());
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        listener.onMessage(eventWithStatus(status), ack);

        assertThat(publisher.published).isEmpty();
        assertThat(ack.acknowledged).isTrue();
    }

    private static PaymentStatusChanged eventWithStatus(String status) {
        UUID paymentId = UUID.randomUUID();
        return PaymentStatusChanged.newBuilder()
                .setEnvelope(
                        EventEnvelope.newBuilder()
                                .setEventId(UUID.randomUUID().toString())
                                .setEventType("payments.payment-status-changed.v1")
                                .setEventVersion(1)
                                .setAggregateId(paymentId.toString())
                                .setAggregateType("payment")
                                .setOccurredAt(Instant.parse("2026-01-01T00:00:00Z"))
                                .setSource("psp-connector")
                                .setTraceId("trace-1")
                                .setCorrelationId("corr-1")
                                .setCausationId(null)
                                .build())
                .setPaymentId(paymentId.toString())
                .setMerchantId("merchant-1")
                .setAmount(BigDecimal.TEN)
                .setCurrency("EUR")
                .setStatus(status)
                .setProviderReference("")
                .setDeclineReason(null)
                .build();
    }

    private static final class RecordingPublisher implements WebhookDeliveryPublisher {
        private final List<WebhookDeliveryCommand> published = new ArrayList<>();

        @Override
        public CompletableFuture<Void> publishNow(
                String topic, WebhookDeliveryCommand command, RetryEnvelope envelope) {
            published.add(command);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> publishDelayed(
                String topic, WebhookDeliveryCommand command, RetryEnvelope envelope, Duration delay) {
            throw new UnsupportedOperationException("planning never delays");
        }
    }

    private static final class RecordingAcknowledgment implements Acknowledgment {
        private boolean acknowledged;

        @Override
        public void acknowledge() {
            acknowledged = true;
        }
    }
}
