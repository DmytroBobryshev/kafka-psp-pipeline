package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.PaymentStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * JPA entity for the {@code payment_status_history} table (M20; schema owned by
 * {@code db/migration/V9__create_payment_status_history_table.sql}, Flyway-managed - ADR-0005).
 * Deliberately NOT {@code @Data} - same rule as {@link PaymentEntity}/{@code RefundEntity}.
 *
 * <p>{@code eventId} is NOT annotated {@code unique = true} here - same convention as
 * psp-connector's {@code PaymentAttemptEntity#inboundEventId}: the docker-compose profile runs
 * Hibernate with {@code ddl-auto=validate}, which only checks column existence/type against
 * Flyway's schema, not constraints, so the real UNIQUE constraint lives solely in the V9
 * migration SQL.
 */
@Entity
@Table(name = "payment_status_history")
@Getter
@Setter
@NoArgsConstructor
public class PaymentStatusHistoryEntity {

    @Id
    private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentStatus status;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "recorded_at", nullable = false)
    private Instant recordedAt;
}
