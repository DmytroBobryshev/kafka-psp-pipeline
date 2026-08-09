package com.example.psp.pspconnector.adapters.out.persistence;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository - the actual SQL-generating layer behind
 * {@link PostgresAttemptLogRepository}. Not a hexagon port itself: the domain never sees this
 * type, only {@link com.example.psp.pspconnector.domain.port.AttemptLogRepository} (ADR-0007).
 */
public interface PaymentAttemptJpaRepository extends JpaRepository<PaymentAttemptEntity, UUID> {
}
