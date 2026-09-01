package com.example.psp.ledger.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;

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
            throw new IllegalArgumentException(
                    "ledger entry amount must be strictly positive, was " + amount.amount());
        }
        this.traceId = requireNonBlank(traceId, "traceId");
        this.correlationId = requireNonBlank(correlationId, "correlationId");
        this.recordedAt = Objects.requireNonNull(recordedAt, "recordedAt must not be null");
    }

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

    public static LedgerEntry debit(
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
                EntryDirection.DEBIT,
                amount,
                traceId,
                correlationId,
                Instant.now());
    }

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
