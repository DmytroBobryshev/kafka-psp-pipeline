package com.example.psp.pspconnector.adapters.out.persistence;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository behind {@link PostgresRefundAttemptLogRepository}. Not a hexagon
 * port itself: the domain never sees this type, only
 * {@link com.example.psp.pspconnector.domain.port.RefundAttemptLogRepository} (ADR-0007).
 */
public interface RefundAttemptJpaRepository extends JpaRepository<RefundAttemptEntity, UUID> {

    /**
     * M5 level 1's idempotency pre-check - hits the unique index backing
     * {@code uq_refund_attempts_causation_event_id} (V3 migration).
     */
    boolean existsByCausationEventId(UUID causationEventId);

    /** M19 drill 9 fix: loads the row the republish-on-redelivery path re-emits from. */
    Optional<RefundAttemptEntity> findByCausationEventId(UUID causationEventId);
}
