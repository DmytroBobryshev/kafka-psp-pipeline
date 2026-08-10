package com.example.psp.pspconnector.adapters.out.persistence;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository - the actual SQL-generating layer behind
 * {@link PostgresAttemptLogRepository}. Not a hexagon port itself: the domain never sees this
 * type, only {@link com.example.psp.pspconnector.domain.port.AttemptLogRepository} (ADR-0007).
 */
public interface PaymentAttemptJpaRepository extends JpaRepository<PaymentAttemptEntity, UUID> {

    /**
     * Derived query backing M5 LEVEL 2's idempotency pre-check - Spring Data generates
     * {@code SELECT EXISTS(... WHERE payment_id = ? AND provider_event_id = ?)} from the method
     * name, hitting {@code idx_payment_attempts_payment_id} plus the unique constraint's implicit
     * index (V1 migration).
     */
    boolean existsByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId);

    /**
     * Derived query backing M5 LEVEL 1's idempotency pre-check - hits the unique index backing
     * {@code uq_payment_attempts_inbound_event_id} (V2 migration).
     */
    boolean existsByInboundEventId(UUID inboundEventId);
}
