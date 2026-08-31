package com.example.psp.paymentapi.adapters.out.persistence;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository behind {@link PostgresPaymentStatusHistoryRepository}. Not a hexagon
 * port itself: the domain never sees this type, only
 * {@code domain.port.PaymentStatusHistoryRepository} (ADR-0007).
 */
public interface PaymentStatusHistoryJpaRepository extends JpaRepository<PaymentStatusHistoryEntity, UUID> {

    /**
     * Backs {@code domain.port.PaymentStatusHistoryRepository#findByPaymentId}'s
     * {@code occurredAt} ascending contract directly - a derived-query method name, no
     * {@code @Query} needed.
     */
    List<PaymentStatusHistoryEntity> findByPaymentIdOrderByOccurredAtAsc(UUID paymentId);
}
