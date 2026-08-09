package com.example.psp.paymentapi.adapters.out.persistence;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository - the actual SQL-generating layer behind
 * {@link PostgresPaymentRepository}. Not a hexagon port itself: the domain never sees this type,
 * only {@link com.example.psp.paymentapi.domain.port.PaymentRepository} (ADR-0007).
 */
public interface PaymentJpaRepository extends JpaRepository<PaymentEntity, UUID> {
}
