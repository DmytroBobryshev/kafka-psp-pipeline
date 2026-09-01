package com.example.psp.ledger.adapters.out.persistence;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface RefundSagaStateJpaRepository extends JpaRepository<RefundSagaStateEntity, UUID> {

    List<RefundSagaStateEntity> findByStatusAndUpdatedAtBefore(String status, Instant cutoff);

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
