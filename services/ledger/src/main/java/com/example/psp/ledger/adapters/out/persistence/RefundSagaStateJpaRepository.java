package com.example.psp.ledger.adapters.out.persistence;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data JPA repository behind {@code refund_saga_state}. Not a hexagon port itself
 * (ADR-0007); {@code domain.port.RefundRepository} is what {@code application/} sees.
 */
public interface RefundSagaStateJpaRepository extends JpaRepository<RefundSagaStateEntity, UUID> {

    /**
     * TTL sweep candidates: {@code updated_at} is set at the same instant a row is written as
     * RESERVED and never touched again while it stays RESERVED, so this is exactly "reservations
     * older than {@code refund.reservation.ttl}" (ADR-0008 rule 6).
     */
    List<RefundSagaStateEntity> findByStatusAndUpdatedAtBefore(String status, Instant cutoff);

    /**
     * THE guarded state transition every refund-saga write in this service funnels through: a
     * compare-and-swap {@code UPDATE ... WHERE refund_id = ? AND status = ?}. Returns the number of
     * rows changed - {@code 1} means the transition fired, {@code 0} means the row was not in
     * {@code expectedStatus} (either it never will be, or something else already moved it), which
     * is how {@code RefundWriteTransaction} tells a real transition apart from a duplicate, an
     * illegal transition, or a lost race against a concurrent attempt at the very same move (the
     * TTL sweeper and a compensating {@code refunds.refund-failed.v1} racing to release the same
     * reservation, for instance) - see ADR-0008 rule 3.
     *
     * <p>JPQL, not native SQL: a bulk entity update needs no Postgres-specific syntax here, unlike
     * {@code MerchantBalanceJpaRepository#applyDelta}'s {@code ON CONFLICT} upsert.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
            "UPDATE RefundSagaStateEntity r SET r.status = :newStatus, r.reason = :reason, "
                    + "r.updatedAt = :now WHERE r.refundId = :refundId AND r.status = :expectedStatus")
    int transitionIfStatus(
            @Param("refundId") UUID refundId,
            @Param("expectedStatus") String expectedStatus,
            @Param("newStatus") String newStatus,
            @Param("reason") String reason,
            @Param("now") Instant now);
}
