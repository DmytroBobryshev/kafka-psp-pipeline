package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.PaymentStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * JPA entity for the {@code payments} table (M3; schema owned by
 * {@code db/migration/V1__create_payments_table.sql}, Flyway-managed - ADR-0005).
 *
 * <p>Deliberately NOT {@code @Data} (PLAN.md rule: {@code @Data} on a JPA entity generates
 * equals/hashCode over every field, which is wrong for an entity - identity should be id-based -
 * and its generated toString() can trigger lazy-loading on associations). {@code @Getter}/
 * {@code @Setter}/{@code @NoArgsConstructor} only. The no-arg constructor and setters are what
 * {@link PaymentPersistenceMapper#toEntity} needs to build one from a domain
 * {@link com.example.psp.paymentapi.domain.model.Payment} - MapStruct's default bean strategy
 * requires both.
 */
@Entity
@Table(name = "payments")
@Getter
@Setter
@NoArgsConstructor
public class PaymentEntity {

    @Id
    private UUID id;

    @Column(name = "merchant_id", nullable = false, length = 255)
    private String merchantId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
