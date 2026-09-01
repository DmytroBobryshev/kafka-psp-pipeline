package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PaymentJpaRepository extends JpaRepository<PaymentEntity, UUID> {

    @Modifying
    @Query("UPDATE PaymentEntity p SET p.status = :status, p.statusUpdatedAt = :at"
            + " WHERE p.id = :paymentId")
    void updateStatus(
            @Param("paymentId") UUID paymentId,
            @Param("status") PaymentStatus status,
            @Param("at") java.time.Instant at);

    @Modifying
    @Query("UPDATE PaymentEntity p SET p.status = :status, p.statusUpdatedAt = :at"
            + " WHERE p.id = :paymentId AND p.status = :requiredCurrentStatus")
    void updateStatusIfCurrentStatus(
            @Param("paymentId") UUID paymentId,
            @Param("status") PaymentStatus status,
            @Param("requiredCurrentStatus") PaymentStatus requiredCurrentStatus,
            @Param("at") java.time.Instant at);

    @Modifying
    @Query("UPDATE PaymentEntity p SET p.status = :status, p.statusUpdatedAt = :at"
            + " WHERE p.id = :paymentId AND p.status IN :allowedCurrentStatuses")
    void updateStatusIfCurrentStatusIn(
            @Param("paymentId") UUID paymentId,
            @Param("status") PaymentStatus status,
            @Param("allowedCurrentStatuses") Collection<PaymentStatus> allowedCurrentStatuses,
            @Param("at") java.time.Instant at);

    @Query(
            value =
                    "SELECT p.* FROM payments p "
                            + "LEFT JOIN merchant_configs mc ON mc.merchant_id = p.merchant_id "
                            + "WHERE p.status IN ('CREATED', 'PENDING') "
                            + "AND p.created_at < CAST(:now AS timestamptz) "
                            + "- (COALESCE(mc.payment_expiration_seconds, 900) * INTERVAL '1 second')",
            nativeQuery = true)
    List<PaymentEntity> findExpirationCandidates(@Param("now") java.time.Instant now);

    @Query(
            "SELECT p FROM PaymentEntity p WHERE (:merchantId IS NULL OR p.merchantId = :merchantId) "
                    + "AND (:status IS NULL OR p.status = :status) ORDER BY p.createdAt DESC")
    Page<PaymentEntity> search(
            @Param("merchantId") String merchantId, @Param("status") PaymentStatus status, Pageable pageable);
}
