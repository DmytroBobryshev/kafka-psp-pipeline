package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.DomainEvent;
import com.example.psp.common.events.EventEnvelope;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Wire shape of {@code payments.payment-status-changed.v1} as webhook-notifier's planner
 * deserializes it - field-for-field identical to psp-connector's producer-side record and
 * ledger's own consumer-side mirror, duplicated per service on purpose (ADR-0007's per-boundary
 * mapper convention; Avro codegen in M9 replaces every hand-written copy of this shape with one
 * generated class).
 *
 * @param status            {@code "SUCCEEDED"} or {@code "DECLINED"} - both are planned as
 *                          webhook deliveries; a merchant cares about a decline as much as a
 *                          success.
 * @param providerReference the provider's own event id - not used by this service; carried only
 *                          because it is part of the topic's contract.
 */
public record PaymentStatusChangedEvent(
        EventEnvelope envelope,
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        UUID providerReference,
        String declineReason)
        implements DomainEvent {}
