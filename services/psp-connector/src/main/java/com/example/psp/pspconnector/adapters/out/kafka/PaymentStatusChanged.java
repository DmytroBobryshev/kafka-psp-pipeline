package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.common.events.DomainEvent;
import com.example.psp.common.events.EventEnvelope;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Concrete event for {@code payments.payment-status-changed.v1} (ADR-0001 topic name, ADR-0002
 * envelope). One flat record: the shared {@link EventEnvelope} plus this event's own domain
 * fields at the top level - there is deliberately no generic {@code payload} field.
 *
 * @param paymentId       the payment this status change is about; also {@code envelope.aggregateId}
 *                         (the UI grouping key per ADR-0002) - but NOT the record key. See
 *                         {@link KafkaPaymentStatusPublisher} for the key.
 * @param merchantId       the owning merchant; IS the record key (ADR-0003) on this topic -
 *                         deliberately different from {@code payments.payment-requested.v1},
 *                         which is keyed by {@code paymentId}. See
 *                         {@link KafkaPaymentStatusPublisher}'s class javadoc for why.
 * @param status            {@code "SUCCEEDED"} or {@code "DECLINED"} - never a raw
 *                         {@link com.example.psp.pspconnector.domain.model.ProviderOutcome} name,
 *                         and never emitted for {@code TIMEOUT} (ADR-0006 category A is not a
 *                         business outcome).
 * @param providerReference the (simulated) provider's own event id for this attempt - the same
 *                         value persisted as {@code provider_event_id} in the dedup table.
 * @param declineReason     populated only when {@code status = "DECLINED"}, else {@code null}.
 */
public record PaymentStatusChanged(
        EventEnvelope envelope,
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        UUID providerReference,
        String declineReason)
        implements DomainEvent {
}
