package com.example.psp.ledger.adapters.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * JPA entity for the {@code ledger_entries} table (schema owned by
 * {@code db/migration/V1__create_ledger_tables.sql}, Flyway-managed - ADR-0005).
 *
 * <p>Deliberately NOT {@code @Data} - same rule as every other entity in this codebase: identity
 * equality only, no generated {@code toString()}.
 *
 * <p>{@code inbound_event_id} is M7's idempotency key and carries
 * {@code uq_ledger_entries_inbound_event_id}. It is {@code NOT NULL} here (unlike
 * {@code psp-connector}'s equivalent column, which had to be nullable because a V1 migration
 * predated it): this table is created with the constraint from the first migration, so there are no
 * legacy rows to accommodate and there is never a legitimate reason to write an entry whose cause
 * is unknown. {@code unique = true} is not declared on the annotation because
 * {@code ddl-auto=validate} only checks column existence and type, so the real constraint lives
 * solely in the migration SQL - same convention as psp-connector.
 */
@Entity
@Table(name = "ledger_entries")
@Getter
@Setter
@NoArgsConstructor
public class LedgerEntryEntity {

    @Id private UUID id;

    @Column(name = "inbound_event_id", nullable = false)
    private UUID inboundEventId;

    @Column(name = "merchant_id", nullable = false, length = 255)
    private String merchantId;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(nullable = false, length = 10)
    private String direction;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "trace_id", nullable = false, length = 255)
    private String traceId;

    @Column(name = "correlation_id", nullable = false, length = 255)
    private String correlationId;

    @Column(name = "recorded_at", nullable = false)
    private Instant recordedAt;
}
