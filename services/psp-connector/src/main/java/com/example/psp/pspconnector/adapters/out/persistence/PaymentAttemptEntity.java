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
 * JPA entity for the {@code payment_attempts} table (schema owned by
 * {@code db/migration/V1__create_payment_attempts_table.sql}, Flyway-managed - ADR-0005).
 *
 * <p>Deliberately NOT {@code @Data} - same rule as {@code payment-api}'s {@code PaymentEntity}:
 * identity-based equality only, and no generated {@code toString()} risking lazy-loading
 * surprises. {@code @Getter}/{@code @Setter}/{@code @NoArgsConstructor} only, which is what
 * {@link PaymentAttemptPersistenceMapper#toEntity} needs for MapStruct's default bean strategy.
 */
@Entity
@Table(name = "payment_attempts")
@Getter
@Setter
@NoArgsConstructor
public class PaymentAttemptEntity {

    @Id
    private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(name = "merchant_id", nullable = false, length = 255)
    private String merchantId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "provider_event_id", nullable = false)
    private UUID providerEventId;

    @Column(nullable = false, length = 20)
    private String outcome;

    @Column(name = "provider_latency_ms", nullable = false)
    private long providerLatencyMs;

    @Column(name = "causation_event_id", nullable = false)
    private UUID causationEventId;

    @Column(name = "trace_id", nullable = false, length = 255)
    private String traceId;

    @Column(name = "correlation_id", nullable = false, length = 255)
    private String correlationId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
