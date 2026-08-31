package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data JPA repository - the actual SQL-generating layer behind
 * {@link PostgresPaymentRepository}. Not a hexagon port itself: the domain never sees this type,
 * only {@link com.example.psp.paymentapi.domain.port.PaymentRepository} (ADR-0007).
 */
public interface PaymentJpaRepository extends JpaRepository<PaymentEntity, UUID> {

    /**
     * M19: an absolute-value {@code UPDATE}, not a {@code save()} of a re-fetched, re-mutated
     * entity - see {@code domain.port.PaymentRepository#updateStatus}'s javadoc for why that
     * shape is what makes this idempotent. {@code @Modifying} is what tells Spring Data this is a
     * DML statement, not a query returning entities; it requires an active transaction, supplied
     * by {@code application.ApplyPaymentOutcomeUseCase}'s {@code @Transactional}.
     */
    @Modifying
    @Query("UPDATE PaymentEntity p SET p.status = :status, p.statusUpdatedAt = :at"
            + " WHERE p.id = :paymentId")
    void updateStatus(
            @Param("paymentId") UUID paymentId,
            @Param("status") PaymentStatus status,
            @Param("at") java.time.Instant at);

    /**
     * M19's transactions-panel query. Both filters are optional: {@code (:x IS NULL OR field =
     * :x)} is the standard JPQL idiom for "match everything when the parameter is null, otherwise
     * match exactly" - one query plan instead of branching between up to four hand-written
     * queries for every combination of filters present/absent. {@code Page<PaymentEntity>} lets
     * Spring Data derive the {@code COUNT} query automatically from this same JPQL (stripping the
     * select list), so {@code PostgresPaymentRepository#search} gets a real total-row count for
     * free, not just this one page's rows.
     */
    @Query(
            "SELECT p FROM PaymentEntity p WHERE (:merchantId IS NULL OR p.merchantId = :merchantId) "
                    + "AND (:status IS NULL OR p.status = :status) ORDER BY p.createdAt DESC")
    Page<PaymentEntity> search(
            @Param("merchantId") String merchantId, @Param("status") PaymentStatus status, Pageable pageable);
}
