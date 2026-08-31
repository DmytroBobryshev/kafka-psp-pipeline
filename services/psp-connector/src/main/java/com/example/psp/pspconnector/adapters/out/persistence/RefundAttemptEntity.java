package com.example.psp.pspconnector.adapters.out.persistence;

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
 * JPA entity for the {@code refund_attempts} table (M11; schema owned by
 * {@code db/migration/V3__create_refund_attempts_table.sql}, Flyway-managed - ADR-0005). The
 * refund-path counterpart of {@code PaymentAttemptEntity}, with one fewer idempotency column - see
 * {@code domain.model.RefundAttempt}'s javadoc for why level 2 is not replicated here.
 */
@Entity
@Table(name = "refund_attempts")
@Getter
@Setter
@NoArgsConstructor
public class RefundAttemptEntity {

    @Id private UUID id;

    @Column(name = "refund_id", nullable = false)
    private UUID refundId;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(name = "merchant_id", nullable = false, length = 255)
    private String merchantId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "provider_reference", nullable = false)
    private UUID providerReference;

    @Column(nullable = false, length = 20)
    private String outcome;

    @Column(name = "provider_latency_ms", nullable = false)
    private long providerLatencyMs;

    // THE idempotency key (M5 level 1) - unique-constrained by the V3 migration. Also the audit
    // trail's causal link, same dual role causation_event_id/inbound_event_id split psp-connector's
    // PaymentAttemptEntity documents (there, deliberately kept as two columns for a reason that
    // does not apply here - this table has no level-2 constraint to keep separate from, so ONE
    // column serves both jobs).
    @Column(name = "causation_event_id", nullable = false)
    private UUID causationEventId;

    // Nullable in db/migration/V4 for pre-fix rows - see RefundAttempt#statusEventId.
    @Column(name = "status_event_id")
    private UUID statusEventId;

    @Column(name = "trace_id", nullable = false, length = 255)
    private String traceId;

    @Column(name = "correlation_id", nullable = false, length = 255)
    private String correlationId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
