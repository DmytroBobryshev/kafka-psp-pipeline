package com.example.psp.webhooknotifier.adapters.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.common.events.avro.EventEnvelope;
import com.example.psp.common.events.avro.RefundStatusChanged;
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

/**
 * M24: same "fakes, not a live broker" style as {@code PaymentStatusChangedListenerTest} - the
 * generated {@code RefundExpiredMapperImpl} has no framework dependency at construction time, so
 * it is instantiated directly, alongside a real {@link PlanWebhookDeliveryUseCase} backed by
 * fakes.
 *
 * <p>Proves the EXPIRED-only ALLOWLIST guard {@link RefundExpiredListener}'s javadoc documents:
 * PENDING/IPN_RECEIVED/VERIFIED (psp-connector's own saga-progress trail on this topic) must never
 * reach the planner - nothing merchant-facing to tell anyone about any of them - but the record
 * must still be acknowledged so the consumer group advances past it instead of redelivering it
 * forever. EXPIRED (payment-api's own refund-expiration sweep verdict, M24) is the one value that
 * IS planned.
 */
class RefundExpiredListenerTest {

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
    void expiredStatusIsPlannedAndAcknowledged() {
        UUID paymentId = UUID.randomUUID();
        UUID refundId = UUID.randomUUID();
        RecordingPublisher publisher = new RecordingPublisher();
        RetryChain chain =
                new RetryChain("base", List.of(new RetryChain.Tier("retry5s", Duration.ofSeconds(5))), "dlq");
        PlanWebhookDeliveryUseCase useCase = new PlanWebhookDeliveryUseCase(publisher, chain);
        RefundExpiredListener listener = new RefundExpiredListener(useCase, new RefundExpiredMapperImpl());
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        listener.onMessage(eventWithStatus(paymentId, refundId, "EXPIRED"), ack);

        assertThat(publisher.published).hasSize(1);
        WebhookDeliveryCommand published = publisher.published.get(0);
        assertThat(published.eventType()).isEqualTo("REFUND_EXPIRED");
        assertThat(published.status()).isEqualTo("EXPIRED");
        assertThat(published.refundId()).isEqualTo(refundId);
        assertThat(published.paymentId()).isEqualTo(paymentId);
        assertThat(published.merchantId()).isEqualTo("merchant-1");
        assertThat(published.declineReason()).isNull();
        assertThat(ack.acknowledged).isTrue();
    }

    private static void assertSkippedAndAcknowledged(String status) {
        RecordingPublisher publisher = new RecordingPublisher();
        RetryChain chain =
                new RetryChain("base", List.of(new RetryChain.Tier("retry5s", Duration.ofSeconds(5))), "dlq");
        PlanWebhookDeliveryUseCase useCase = new PlanWebhookDeliveryUseCase(publisher, chain);
        RefundExpiredListener listener = new RefundExpiredListener(useCase, new RefundExpiredMapperImpl());
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        listener.onMessage(eventWithStatus(UUID.randomUUID(), UUID.randomUUID(), status), ack);

        assertThat(publisher.published).isEmpty();
        assertThat(ack.acknowledged).isTrue();
    }

    private static RefundStatusChanged eventWithStatus(UUID paymentId, UUID refundId, String status) {
        return RefundStatusChanged.newBuilder()
                .setEnvelope(
                        EventEnvelope.newBuilder()
                                .setEventId(UUID.randomUUID().toString())
                                .setEventType("refunds.refund-status-changed.v1")
                                .setEventVersion(1)
                                .setAggregateId(refundId.toString())
                                .setAggregateType("refund")
                                .setOccurredAt(Instant.parse("2026-01-01T00:00:00Z"))
                                .setSource("payment-api")
                                .setTraceId("trace-1")
                                .setCorrelationId("corr-1")
                                .setCausationId(null)
                                .build())
                .setRefundId(refundId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId("merchant-1")
                .setAmount(BigDecimal.TEN)
                .setCurrency("EUR")
                .setStatus(status)
                .setProviderReference("")
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
