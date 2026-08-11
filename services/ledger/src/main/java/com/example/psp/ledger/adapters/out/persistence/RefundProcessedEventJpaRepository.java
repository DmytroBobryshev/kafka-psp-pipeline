package com.example.psp.ledger.adapters.out.persistence;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository for {@code refund_processed_events} - the idempotency dedup table
 * shared by every refund-saga listener in this service. Not a hexagon port itself (ADR-0007);
 * {@code existsById} is inherited from {@link JpaRepository} and is exactly M5/M7's check-first
 * pre-check ({@code domain.port.RefundRepository#hasProcessedInboundEvent}).
 */
public interface RefundProcessedEventJpaRepository extends JpaRepository<RefundProcessedEventEntity, UUID> {
}
