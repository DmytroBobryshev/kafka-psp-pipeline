package com.example.psp.webhooknotifier.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.common.events.avro.EventEnvelope;
import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.webhooknotifier.adapters.in.kafka.RefundCompletedMapperImpl;
import com.example.psp.webhooknotifier.adapters.in.kafka.RefundFailedMapperImpl;
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

/**
 * M19: the new planning path - {@code refunds.refund-completed.v1}/{@code refunds.refund-failed.v1}
 * -&gt; {@code adapters.in.kafka.RefundCompletedMapper}/{@code RefundFailedMapper} -&gt;
 * {@link PlanWebhookDeliveryUseCase} -&gt; a planned delivery on the base topic. Same "fakes, not a
 * live broker" style as {@code ExecuteWebhookDeliveryUseCaseTest}: no Spring context, no Kafka -
 * the MapStruct mappers' generated {@code *Impl} classes are plain POJOs with no framework
 * dependency at construction time, so they are instantiated directly here exactly like any other
 * fake.
 *
 * <p>{@link PlanWebhookDeliveryUseCase} itself is reused completely unchanged for both new event
 * types (see its javadoc) - the property genuinely worth proving here is that each mapper produces
 * a correctly-shaped {@link WebhookDeliveryCommand} (right {@code eventType}, right {@code status}
 * vocabulary, {@code refundId} populated, {@code declineReason} reused for the refund failure
 * reason) and that the use case publishes it to the SAME base topic a payment-status delivery
 * would use, attempt 1, no different from any other planned delivery.
 */
class RefundWebhookPlanningTest {

    private static final RetryChain CHAIN =
            new RetryChain("base", List.of(new RetryChain.Tier("retry5s", Duration.ofSeconds(5))), "dlq");

    private final RefundCompletedMapperImpl refundCompletedMapper = new RefundCompletedMapperImpl();
    private final RefundFailedMapperImpl refundFailedMapper = new RefundFailedMapperImpl();

    @Test
    void refundCompletedIsPlannedAsAWebhookDeliveryWithNoDeclineReason() {
        UUID paymentId = UUID.randomUUID();
        UUID refundId = UUID.randomUUID();
        RefundCompleted event = refundCompleted(paymentId, refundId);

        WebhookDeliveryCommand command = refundCompletedMapper.toCommand(event);
        RecordingPublisher publisher = new RecordingPublisher();
        PlanWebhookDeliveryUseCase useCase = new PlanWebhookDeliveryUseCase(publisher, CHAIN);

        useCase.execute(command);

        assertThat(publisher.published).hasSize(1);
        Published published = publisher.published.get(0);
        assertThat(published.topic()).isEqualTo("base");
        assertThat(published.envelope().attemptCount()).isEqualTo(1);

        WebhookDeliveryCommand published0 = published.command();
        assertThat(published0.eventType()).isEqualTo("REFUND_COMPLETED");
        assertThat(published0.refundId()).isEqualTo(refundId);
        assertThat(published0.paymentId()).isEqualTo(paymentId);
        assertThat(published0.merchantId()).isEqualTo("merchant-1");
        assertThat(published0.status()).isEqualTo("COMPLETED");
        assertThat(published0.declineReason()).isNull();
        assertThat(published0.amount()).isEqualByComparingTo(BigDecimal.TEN);
        assertThat(published0.currency()).isEqualTo("EUR");
        assertThat(published0.causationEventId()).isEqualTo(UUID.fromString(event.getEnvelope().getEventId()));
    }

    @Test
    void refundFailedIsPlannedAsAWebhookDeliveryCarryingTheFailureReason() {
        UUID paymentId = UUID.randomUUID();
        UUID refundId = UUID.randomUUID();
        RefundFailed event = refundFailed(paymentId, refundId, "INSUFFICIENT_BALANCE");

        WebhookDeliveryCommand command = refundFailedMapper.toCommand(event);
        RecordingPublisher publisher = new RecordingPublisher();
        PlanWebhookDeliveryUseCase useCase = new PlanWebhookDeliveryUseCase(publisher, CHAIN);

        useCase.execute(command);

        assertThat(publisher.published).hasSize(1);
        WebhookDeliveryCommand published0 = publisher.published.get(0).command();
        assertThat(published0.eventType()).isEqualTo("REFUND_FAILED");
        assertThat(published0.refundId()).isEqualTo(refundId);
        assertThat(published0.paymentId()).isEqualTo(paymentId);
        assertThat(published0.status()).isEqualTo("FAILED");
        // The refund failure reason travels in the same slot a payment decline reason would -
        // see WebhookDeliveryCommand#declineReason()'s widened javadoc.
        assertThat(published0.declineReason()).isEqualTo("INSUFFICIENT_BALANCE");
    }

    private static RefundCompleted refundCompleted(UUID paymentId, UUID refundId) {
        return RefundCompleted.newBuilder()
                .setEnvelope(envelope(refundId, "refunds.refund-completed.v1"))
                .setRefundId(refundId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId("merchant-1")
                .setAmount(BigDecimal.TEN)
                .setCurrency("EUR")
                .setProviderReference(UUID.randomUUID().toString())
                .build();
    }

    private static RefundFailed refundFailed(UUID paymentId, UUID refundId, String reason) {
        return RefundFailed.newBuilder()
                .setEnvelope(envelope(refundId, "refunds.refund-failed.v1"))
                .setRefundId(refundId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId("merchant-1")
                .setAmount(BigDecimal.TEN)
                .setCurrency("EUR")
                .setReason(reason)
                .build();
    }

    private static EventEnvelope envelope(UUID aggregateId, String eventType) {
        return EventEnvelope.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(eventType)
                .setEventVersion(1)
                .setAggregateId(aggregateId.toString())
                .setAggregateType("refund")
                .setOccurredAt(Instant.parse("2026-01-01T00:00:00Z"))
                .setSource("psp-connector")
                .setTraceId("trace-1")
                .setCorrelationId("corr-1")
                .setCausationId(null)
                .build();
    }

    private record Published(String topic, WebhookDeliveryCommand command, RetryEnvelope envelope) {}

    private static final class RecordingPublisher implements WebhookDeliveryPublisher {
        private final List<Published> published = new ArrayList<>();

        @Override
        public CompletableFuture<Void> publishNow(String topic, WebhookDeliveryCommand command, RetryEnvelope envelope) {
            published.add(new Published(topic, command, envelope));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> publishDelayed(
                String topic, WebhookDeliveryCommand command, RetryEnvelope envelope, Duration delay) {
            throw new UnsupportedOperationException("planning never delays");
        }
    }
}
