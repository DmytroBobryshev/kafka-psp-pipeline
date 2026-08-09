package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.common.events.DomainEvent;
import com.example.psp.common.events.EventEnvelope;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Wire shape of {@code payments.payment-requested.v1} as psp-connector deserializes it - the
 * consuming side's mirror of {@code payment-api}'s {@code PaymentRequested} producer record.
 * Field-for-field identical on purpose: both are hand-written JSON-era records of the same
 * ADR-0001/ADR-0002 contract, duplicated per service rather than shared, same as every other
 * concrete event type in this codebase (only {@link EventEnvelope} itself lives in
 * {@code libs/common-events}). Avro codegen in M9 replaces both with one generated class.
 */
public record PaymentRequestedEvent(
        EventEnvelope envelope,
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status)
        implements DomainEvent {
}
