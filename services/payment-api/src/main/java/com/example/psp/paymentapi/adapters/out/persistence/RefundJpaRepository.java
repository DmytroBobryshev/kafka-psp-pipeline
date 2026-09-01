package com.example.psp.paymentapi.adapters.out.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface RefundJpaRepository extends JpaRepository<RefundEntity, UUID> {

    List<RefundEntity> findByPaymentId(UUID paymentId);

    @Query("SELECT COALESCE(SUM(r.amount), 0) FROM RefundEntity r WHERE r.paymentId = :paymentId")
    BigDecimal sumAmountByPaymentId(@Param("paymentId") UUID paymentId);

    Optional<RefundEntity> findByIdAndPaymentId(UUID id, UUID paymentId);

    @Query(
            value =
                    "SELECT r.* FROM refunds r "
                            + "LEFT JOIN merchant_configs mc ON mc.merchant_id = r.merchant_id "
                            + "WHERE r.created_at < CAST(:now AS timestamptz) "
                            + "- (COALESCE(mc.refund_expiration_seconds, 900) * INTERVAL '1 second') "
                            + "AND NOT EXISTS ("
                            + "  SELECT 1 FROM refund_status_history h "
                            + "  WHERE h.refund_id = r.id AND h.status IN ('COMPLETED', 'FAILED', 'EXPIRED')"
                            + ")",
            nativeQuery = true)
    List<RefundEntity> findExpirationCandidates(@Param("now") Instant now);
}
