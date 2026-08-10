package com.example.psp.ledger.domain.model;

import java.time.Instant;
import java.util.Objects;

/**
 * A merchant's running balance as it stands after some set of {@link LedgerEntry} rows have been
 * applied. Pure Java, no framework dependency (ADR-0007).
 *
 * <p>This is a <b>read model of committed state</b>, not something the domain mutates: the balance
 * is only ever changed by the atomic "insert entry + add delta" write in the persistence adapter,
 * and this record is what that write hands back. It exists so the outbound
 * {@code ledger.ledger-entry-recorded.v1} event can carry {@code balanceAfter} - the value that
 * makes the published entry self-describing for analytics (M10) and the audit sink (M13) without
 * anyone querying this service's database (ADR-0005 forbids that anyway).
 *
 * <p>{@code entryCount} is deliberately part of the model rather than a derived
 * {@code SELECT count(*)}: it is incremented in the same statement as the balance, so a
 * balance/entry-count pair that disagrees with {@code ledger_entries} is a loud signal that
 * something wrote the balance outside the intended path.
 *
 * <p>Single-writer note (ADR-0003): {@code payments.payment-status-changed.v1} is keyed by
 * {@code merchantId}, so every event affecting one merchant's balance lands on one partition and
 * is therefore processed by exactly one consumer instance at a time. That is why M7 is about Kafka
 * transactions rather than about row-level contention - but the persistence adapter still uses an
 * atomic read-modify-write (Postgres {@code ON CONFLICT ... DO UPDATE}) rather than
 * read-then-write in Java, because "one writer" is a property of the current partitioning, not an
 * invariant the database itself enforces.
 */
public record MerchantBalance(String merchantId, Money balance, long entryCount, Instant updatedAt) {

    public MerchantBalance {
        Objects.requireNonNull(merchantId, "merchantId must not be null");
        if (merchantId.isBlank()) {
            throw new IllegalArgumentException("merchantId must not be blank");
        }
        Objects.requireNonNull(balance, "balance must not be null");
        if (entryCount < 0) {
            throw new IllegalArgumentException("entryCount must not be negative, was " + entryCount);
        }
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
    }
}
