package com.example.psp.ledger.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

/**
 * One immutable double-entry-style line in the ledger: "this inbound event moved this merchant's
 * balance by this much, in this direction". Pure Java, no framework dependency (ADR-0007) - Lombok
 * is allowed in {@code domain/} because it is compile-time-only.
 *
 * <h2>{@code inboundEventId} is the idempotency key, and that is the whole point of M7</h2>
 *
 * <p>{@code inboundEventId} is the inbound {@code payments.payment-status-changed.v1} envelope's
 * own {@code eventId} (ADR-0002: "UUID v7, producer-generated, <b>the</b> idempotency key for
 * consumer dedup"). It is carried on the entry itself, persisted in a column with a
 * <b>unique constraint</b> ({@code uq_ledger_entries_inbound_event_id}, V1 migration), and that
 * constraint - not the Kafka transaction - is what makes a merchant balance correct under replay.
 *
 * <p>Worth stating explicitly because it is the single most confusable thing in this module: the
 * Kafka transaction this entry is published inside covers <b>Kafka to Kafka only</b> (the produced
 * record plus the consumed offsets). It does not, and cannot, extend to the Postgres row this
 * entry becomes. If the transactional producer were deleted tomorrow and this service went back to
 * a plain producer, balances would still never double-count. If this unique constraint were
 * deleted instead, no amount of Kafka exactly-once would save them. See README.md's "Where Kafka
 * EOS ends".
 *
 * <p>{@code traceId}/{@code correlationId} are carried on the entry rather than passed separately
 * so that (a) the persisted row is a self-contained audit trail matching the event that caused it,
 * and (b) {@code domain.port.LedgerEntryPublisher} can build a causally-chained
 * {@code EventEnvelope} from the entry alone, without depending on an {@code application/} command
 * type (a port signature may only reference {@code domain/} types - ADR-0007).
 */
@Getter
public final class LedgerEntry {

    private final UUID id;
    private final UUID inboundEventId;
    private final String merchantId;
    private final UUID paymentId;
    private final EntryDirection direction;
    private final Money amount;
    private final String traceId;
    private final String correlationId;
    private final Instant recordedAt;

    private LedgerEntry(
            UUID id,
            UUID inboundEventId,
            String merchantId,
            UUID paymentId,
            EntryDirection direction,
            Money amount,
            String traceId,
            String correlationId,
            Instant recordedAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.inboundEventId = Objects.requireNonNull(inboundEventId, "inboundEventId must not be null");
        this.merchantId = requireNonBlank(merchantId, "merchantId");
        this.paymentId = Objects.requireNonNull(paymentId, "paymentId must not be null");
        this.direction = Objects.requireNonNull(direction, "direction must not be null");
        this.amount = Objects.requireNonNull(amount, "amount must not be null");
        if (amount.amount().signum() <= 0) {
            // A single entry always carries a POSITIVE magnitude; EntryDirection carries the sign.
            // A signed entry amount plus a direction would let the same movement be expressed two
            // ways (DEBIT +10 vs CREDIT -10), which is exactly how ledgers acquire silent bugs.
            throw new IllegalArgumentException(
                    "ledger entry amount must be strictly positive, was " + amount.amount());
        }
        this.traceId = requireNonBlank(traceId, "traceId");
        this.correlationId = requireNonBlank(correlationId, "correlationId");
        this.recordedAt = Objects.requireNonNull(recordedAt, "recordedAt must not be null");
    }

    /** A successful capture: money into the merchant balance. The only entry M7 itself produces. */
    public static LedgerEntry credit(
            UUID inboundEventId,
            String merchantId,
            UUID paymentId,
            Money amount,
            String traceId,
            String correlationId) {
        return new LedgerEntry(
                UUID.randomUUID(),
                inboundEventId,
                merchantId,
                paymentId,
                EntryDirection.CREDIT,
                amount,
                traceId,
                correlationId,
                Instant.now());
    }

    /**
     * Reconstitutes an entry that already exists in storage - used by the persistence adapter's
     * mapper, never by {@code application/}.
     */
    public static LedgerEntry reconstitute(
            UUID id,
            UUID inboundEventId,
            String merchantId,
            UUID paymentId,
            EntryDirection direction,
            Money amount,
            String traceId,
            String correlationId,
            Instant recordedAt) {
        return new LedgerEntry(
                id,
                inboundEventId,
                merchantId,
                paymentId,
                direction,
                amount,
                traceId,
                correlationId,
                recordedAt);
    }

    /** The signed amount this entry adds to the merchant balance ({@code +amount} / {@code -amount}). */
    public BigDecimal signedAmount() {
        return amount.amount().multiply(BigDecimal.valueOf(direction.sign()));
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
