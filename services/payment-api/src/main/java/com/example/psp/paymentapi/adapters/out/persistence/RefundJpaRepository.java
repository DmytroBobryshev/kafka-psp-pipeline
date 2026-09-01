package com.example.psp.paymentapi.adapters.out.persistence;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data JPA repository behind {@link PostgresRefundRepository}. Not a hexagon port itself:
 * the domain never sees this type, only {@code domain.port.RefundRepository} (ADR-0007).
 */
public interface RefundJpaRepository extends JpaRepository<RefundEntity, UUID> {

    List<RefundEntity> findByPaymentId(UUID paymentId);

    @Query("SELECT COALESCE(SUM(r.amount), 0) FROM RefundEntity r WHERE r.paymentId = :paymentId")
    BigDecimal sumAmountByPaymentId(@Param("paymentId") UUID paymentId);

    /** M23: backs {@code domain.port.RefundRepository#findByIdAndPaymentId} directly. */
    Optional<RefundEntity> findByIdAndPaymentId(UUID id, UUID paymentId);
}
