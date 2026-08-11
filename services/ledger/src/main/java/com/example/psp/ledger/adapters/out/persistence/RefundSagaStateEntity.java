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
 * JPA entity for {@code refund_saga_state} (M11; schema owned by
 * {@code db/migration/V2__create_refund_saga_tables.sql}, Flyway-managed - ADR-0005). The
 * mutable state-machine row this service's whole refund saga participation revolves around - one
 * row per {@code refundId}, transitioned in place by the guarded compare-and-swap queries on
 * {@link RefundSagaStateJpaRepository}, never by a naive load-modify-save (that would race against
 * a concurrent transition attempting the same or a different move).
 *
 * <p>{@code status} is stored as {@code VARCHAR}, not a native enum type, mapped to/from
 * {@code domain.model.RefundSagaStatus} by name in {@link RefundPersistenceMapper} - same
 * convention as {@code LedgerEntryEntity.direction}.
 */
@Entity
@Table(name = "refund_saga_state")
@Getter
@Setter
@NoArgsConstructor
public class RefundSagaStateEntity {

    @Id
    @Column(name = "refund_id")
    private UUID refundId;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(name = "merchant_id", nullable = false, length = 255)
    private String merchantId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(nullable = false, length = 30)
    private String status;

    @Column(length = 255)
    private String reason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
