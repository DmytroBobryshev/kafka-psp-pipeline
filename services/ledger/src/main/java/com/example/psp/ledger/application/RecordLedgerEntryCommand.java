package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.Money;
import java.util.UUID;

/**
 * Application-layer input model for {@link RecordLedgerEntryUseCase}. Deliberately separate from
 * {@code adapters.in.kafka.PaymentStatusChangedEvent} - the Kafka adapter maps its wire event onto
 * this command, so the use case never depends on the wire contract (same pattern as
 * {@code psp-connector}'s {@code ProcessPaymentRequestCommand}).
 *
 * @param inboundEventId the inbound {@code payments.payment-status-changed.v1} envelope's own
 *                       {@code eventId}. <b>The</b> idempotency key (ADR-0002), and the reason
 *                       replaying the topic cannot double a balance. Also becomes the outbound
 *                       envelope's {@code causationId}. Stable across replays, rebalances, aborted
 *                       Kafka transactions and offset resets - which is precisely the property
 *                       that makes it usable as a dedup key and that nothing minted during
 *                       processing has.
 * @param paymentId      the payment this status change is about (the inbound envelope's
 *                       {@code aggregateId}); carried onto the ledger entry for audit and for the
 *                       M17 UI's payment timeline.
 * @param merchantId     the merchant whose balance moves. Also the inbound record's <b>key</b>
 *                       (ADR-0003), which is what gives this service a single in-flight writer per
 *                       balance.
 * @param amount         the captured amount.
 * @param status         the inbound status verbatim ({@code "SUCCEEDED"} / {@code "DECLINED"}).
 *                       Interpreting it is the use case's job, not the adapter's - see
 *                       {@link RecordLedgerEntryUseCase#execute}.
 * @param traceId        propagated from the inbound envelope (real W3C propagation is M15).
 * @param correlationId  propagated from the inbound envelope.
 */
public record RecordLedgerEntryCommand(
        UUID inboundEventId,
        UUID paymentId,
        String merchantId,
        Money amount,
        String status,
        String traceId,
        String correlationId) {
}
