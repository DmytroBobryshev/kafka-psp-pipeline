package com.example.psp.ledger.adapters.out.persistence;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository for {@code refund_reservations}. Not a hexagon port itself: the
 * domain never sees this type, only {@code domain.port.RefundRepository} (ADR-0007).
 */
public interface RefundReservationJpaRepository extends JpaRepository<RefundReservationEntity, UUID> {
}
