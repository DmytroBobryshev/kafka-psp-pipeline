package com.example.psp.ledger.adapters.out.kafka;

import com.example.psp.common.events.DomainEvent;
import com.example.psp.common.events.EventEnvelope;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Concrete event for {@code ledger.ledger-entry-recorded.v1} (ADR-0001 topic name, ADR-0002
 * envelope). One flat record: the shared {@link EventEnvelope} plus this event's own domain fields
 * at the top level - there is deliberately no generic {@code payload} field.
 *
 * <p>Per {@code docs/diagrams/topic-map.md} this topic is keyed by {@code merchantId}, 6
 * partitions, 30-day retention (it is the audit trail; the Connect Mongo sink in M13 reads it),
 * consumed by analytics, realtime-gateway and that sink.
 *
 * @param envelope     {@code envelope.causationId} is the inbound
 *                     {@code payments.payment-status-changed.v1} {@code eventId} - the same value
 *                     stored in {@code ledger_entries.inbound_event_id}, so the causal chain and
 *                     the idempotency key are provably the same id.
 *                     {@code envelope.aggregateId} is the {@code merchantId}: a ledger entry is a
 *                     fact about a merchant's balance. That makes aggregateId and the record key
 *                     coincide on this topic, which is unusual in this system (ADR-0003) and worth
 *                     noticing rather than generalising from.
 * @param entryId      the ledger entry's own id ({@code ledger_entries.id}).
 * @param paymentId    the payment that caused the movement; carried so consumers can join back to
 *                     the payment timeline without querying this service (ADR-0004/0005).
 * @param direction    {@code "CREDIT"} or {@code "DEBIT"}; {@code amount} is always positive.
 * @param balanceAfter the merchant's balance after this entry was applied. Published so downstream
 *                     never needs to read the ledger's database. Note it is a snapshot of committed
 *                     Postgres state at the moment of publication - if this record ends up in an
 *                     aborted Kafka transaction, the Postgres row it describes still exists. That
 *                     asymmetry is the module's whole point (README, "Where Kafka EOS ends").
 */
public record LedgerEntryRecorded(
        EventEnvelope envelope,
        UUID entryId,
        String merchantId,
        UUID paymentId,
        String direction,
        BigDecimal amount,
        String currency,
        BigDecimal balanceAfter,
        Instant recordedAt)
        implements DomainEvent {
}
