package com.example.psp.paymentapi.adapters.out.persistence;

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
 * JPA entity for the {@code refunds} table (M11; schema owned by
 * {@code db/migration/V4__create_refunds_table.sql}, Flyway-managed - ADR-0005). Deliberately NOT
 * {@code @Data} - same rule as {@link PaymentEntity}.
 */
@Entity
@Table(name = "refunds")
@Getter
@Setter
@NoArgsConstructor
public class RefundEntity {

    @Id private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(name = "merchant_id", nullable = false, length = 255)
    private String merchantId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(length = 500)
    private String reason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
