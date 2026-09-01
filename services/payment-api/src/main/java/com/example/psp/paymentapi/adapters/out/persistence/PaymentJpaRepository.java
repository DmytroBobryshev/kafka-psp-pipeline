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
     * M20: the NO-DOWNGRADE guard behind {@code domain.port.PaymentRepository#applyPendingStatus}
     * - identical shape to {@link #updateStatus} above, plus one extra {@code AND} clause. Both
     * {@code :status} and {@code :requiredCurrentStatus} are bound parameters rather than JPQL
     * enum literals (avoids embedding {@link PaymentStatus}'s fully-qualified name in a query
     * string) - the adapter always calls this with {@code PENDING}/{@code CREATED}, but the query
     * itself stays a general "conditional absolute UPDATE" rather than a PENDING-only one, in case
     * a future FROM/TO pair needs the identical guard shape.
     */
    @Modifying
    @Query("UPDATE PaymentEntity p SET p.status = :status, p.statusUpdatedAt = :at"
            + " WHERE p.id = :paymentId AND p.status = :requiredCurrentStatus")
    void updateStatusIfCurrentStatus(
            @Param("paymentId") UUID paymentId,
            @Param("status") PaymentStatus status,
            @Param("requiredCurrentStatus") PaymentStatus requiredCurrentStatus,
            @Param("at") java.time.Instant at);

    /**
     * M22: the IN-list generalisation of {@link #updateStatusIfCurrentStatus} behind
     * {@code domain.port.PaymentRepository#applyExpiredStatus} - identical shape, plus the guard
     * clause taking a set of acceptable FROM states (CREATED, PENDING) instead of exactly one,
     * since EXPIRED may legally apply from either. A second, always-{@code IN}-based query rather
     * than two calls to {@link #updateStatusIfCurrentStatus} for the same reason
     * {@code PaymentJpaRepository#search} takes optional filters instead of branching between
     * hand-written queries: one round trip, one query plan.
     */
    @Modifying
    @Query("UPDATE PaymentEntity p SET p.status = :status, p.statusUpdatedAt = :at"
            + " WHERE p.id = :paymentId AND p.status IN :allowedCurrentStatuses")
    void updateStatusIfCurrentStatusIn(
            @Param("paymentId") UUID paymentId,
            @Param("status") PaymentStatus status,
            @Param("allowedCurrentStatuses") Collection<PaymentStatus> allowedCurrentStatuses,
            @Param("at") java.time.Instant at);

    /**
     * M22: {@code adapters.in.scheduler.PaymentExpirationScheduler}'s candidate query -
     * {@code domain.port.PaymentRepository#findExpirationCandidates}'s real implementation.
     * Native SQL (JPQL has no clean way to join an unrelated entity's table by a foreign key that
     * is not a mapped association - {@code PaymentEntity} deliberately has none to
     * {@code MerchantConfigEntity}, same "no FK, the join is a query-time convenience only" shape
     * as {@code PaymentJpaRepository#search}'s optional filters). {@code LEFT JOIN} +
     * {@code COALESCE(..., 900)}: a merchant with no {@code merchant_configs} row at all (never
     * called {@code PUT /api/merchants/{id}/config}) still gets the same 900s default
     * {@link com.example.psp.paymentapi.domain.model.MerchantConfig#DEFAULT_PAYMENT_EXPIRATION_SECONDS}
     * names as everyone else, rather than being invisible to the sweep forever.
     */
    @Query(
            value =
                    "SELECT p.* FROM payments p "
                            + "LEFT JOIN merchant_configs mc ON mc.merchant_id = p.merchant_id "
                            + "WHERE p.status IN ('CREATED', 'PENDING') "
                            + "AND p.created_at < CAST(:now AS timestamptz) "
                            + "- (COALESCE(mc.payment_expiration_seconds, 900) * INTERVAL '1 second')",
            nativeQuery = true)
    List<PaymentEntity> findExpirationCandidates(@Param("now") java.time.Instant now);

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
