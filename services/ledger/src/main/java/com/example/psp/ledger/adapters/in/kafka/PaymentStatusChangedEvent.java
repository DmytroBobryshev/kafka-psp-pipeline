package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.DomainEvent;
import com.example.psp.common.events.EventEnvelope;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Wire shape of {@code payments.payment-status-changed.v1} as the ledger deserializes it - the
 * consuming side's mirror of {@code psp-connector}'s {@code PaymentStatusChanged} producer record.
 * Field-for-field identical on purpose: both are hand-written JSON-era records of the same
 * ADR-0001/ADR-0002 contract, duplicated per service rather than shared, exactly as
 * {@code psp-connector}'s own {@code PaymentRequestedEvent} mirrors {@code payment-api}'s
 * {@code PaymentRequested}. Only {@link EventEnvelope} itself lives in
 * {@code libs/common-events}; Avro codegen in M9 replaces both sides with one generated class.
 *
 * @param envelope          ADR-0002 envelope. {@code envelope.eventId()} is the ledger's
 *                          idempotency key - see {@code domain.model.LedgerEntry}.
 * @param status            {@code "SUCCEEDED"} or {@code "DECLINED"}. Only the former moves money.
 * @param providerReference the provider's own event id for the attempt; carried for audit only -
 *                          the ledger deliberately does NOT dedup on it, because it is minted
 *                          during processing upstream and is not stable across replays (the exact
 *                          defect {@code psp-connector}'s M5 section documents).
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
        implements DomainEvent {
}
