package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.Money;
import java.util.UUID;

/**
 * Application-layer input for {@link ReserveRefundUseCase} - the Kafka adapter maps the inbound
 * {@code refunds.refund-requested.v1} event onto this command, same pattern as
 * {@code RecordLedgerEntryCommand} (M7).
 *
 * @param inboundEventId the inbound envelope's own {@code eventId} - the idempotency key (ADR-0002).
 * @param refundId       the saga's correlation id (envelope.aggregateId).
 * @param paymentId      the payment being refunded.
 * @param merchantId     the merchant whose balance is reserved against; also the inbound record's
 *                       key (ADR-0003).
 * @param amount         the amount to reserve.
 * @param traceId        propagated from the inbound envelope.
 * @param correlationId  propagated from the inbound envelope.
 */
public record ReserveRefundCommand(
        UUID inboundEventId,
        UUID refundId,
        UUID paymentId,
        String merchantId,
        Money amount,
        String traceId,
        String correlationId) {
}
