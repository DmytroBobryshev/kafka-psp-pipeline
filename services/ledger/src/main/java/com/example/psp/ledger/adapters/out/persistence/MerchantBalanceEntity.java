package com.example.psp.ledger.adapters.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * JPA entity for the {@code merchant_balances} table - one row per merchant, the running total of
 * every {@link LedgerEntryEntity} applied so far. Schema owned by
 * {@code db/migration/V1__create_ledger_tables.sql} (ADR-0005).
 *
 * <p>This entity is used for <b>reads only</b>. The write is a native
 * {@code INSERT ... ON CONFLICT DO UPDATE} in {@link MerchantBalanceJpaRepository} rather than a
 * JPA load-modify-save: read-modify-write through the persistence context would turn a single
 * atomic statement into a lost-update race whose window is the length of the enclosing
 * transaction. ADR-0003's single-writer-per-merchant partitioning makes that race unlikely, but
 * "unlikely because of how a topic happens to be keyed today" is not an invariant a balance table
 * should rely on.
 *
 * <p>{@code merchant_id} is the primary key: a merchant has exactly one balance, and the PK's
 * implicit unique index is what the upsert's {@code ON CONFLICT} clause targets.
 */
@Entity
@Table(name = "merchant_balances")
@Getter
@Setter
@NoArgsConstructor
public class MerchantBalanceEntity {

    @Id
    @Column(name = "merchant_id", nullable = false, length = 255)
    private String merchantId;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal balance;

    @Column(name = "entry_count", nullable = false)
    private long entryCount;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
