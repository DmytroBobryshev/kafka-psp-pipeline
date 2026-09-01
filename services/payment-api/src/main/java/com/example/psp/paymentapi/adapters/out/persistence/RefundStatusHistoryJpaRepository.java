package com.example.psp.paymentapi.adapters.out.persistence;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository behind {@link PostgresRefundStatusHistoryRepository}. Not a hexagon
 * port itself: the domain never sees this type (ADR-0007).
 */
public interface RefundStatusHistoryJpaRepository extends JpaRepository<RefundStatusHistoryEntity, UUID> {

    List<RefundStatusHistoryEntity> findByRefundIdOrderByOccurredAtAsc(UUID refundId);
}
