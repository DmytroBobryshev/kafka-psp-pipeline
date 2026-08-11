package com.example.psp.ledger.adapters.out.persistence;

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
 * JPA entity for {@code refund_processed_events} (M11; schema owned by
 * {@code db/migration/V2__create_refund_saga_tables.sql}, Flyway-managed - ADR-0005). The
 * idempotency ledger shared across all three refund-saga listeners in this service
 * ({@code refunds.refund-requested.v1}, {@code refunds.refund-completed.v1},
 * {@code refunds.refund-failed.v1}) - one row per successfully-processed inbound {@code eventId},
 * the M5/M7 dedup pattern generalised across a saga with three distinct consumption points instead
 * of one. {@code inboundEventId} is the primary key: the unique constraint IS the idempotency
 * guarantee (same role {@code ledger_entries.inbound_event_id} plays for M7).
 */
@Entity
@Table(name = "refund_processed_events")
@Getter
@Setter
@NoArgsConstructor
public class RefundProcessedEventEntity {

    @Id
    @Column(name = "inbound_event_id")
    private UUID inboundEventId;

    @Column(name = "refund_id", nullable = false)
    private UUID refundId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
