package com.example.psp.paymentapi.adapters.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * JPA entity for the {@code refund_status_history} table (M23; schema owned by
 * {@code db/migration/V12__create_refund_status_history_table.sql}, Flyway-managed - ADR-0005).
 * Deliberately NOT {@code @Data} - same rule as {@link PaymentStatusHistoryEntity}.
 *
 * <p>{@code eventId} is NOT annotated {@code unique = true} here - same convention as
 * {@link PaymentStatusHistoryEntity#getEventId()}: {@code ddl-auto=validate} only checks column
 * existence/type, the real UNIQUE constraint lives solely in the V12 migration SQL.
 */
@Entity
@Table(name = "refund_status_history")
@Getter
@Setter
@NoArgsConstructor
public class RefundStatusHistoryEntity {

    @Id
    private UUID id;

    @Column(name = "refund_id", nullable = false)
    private UUID refundId;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(nullable = false, length = 20)
    private String status;

    @Column(name = "provider_reference", length = 64)
    private String providerReference;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "recorded_at", nullable = false)
    private Instant recordedAt;
}
