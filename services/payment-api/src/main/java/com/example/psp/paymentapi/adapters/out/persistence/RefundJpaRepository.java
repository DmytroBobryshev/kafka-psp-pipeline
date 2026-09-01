package com.example.psp.paymentapi.adapters.out.persistence;

import java.math.BigDecimal;
import java.time.Instant;
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

    /**
     * M24: {@code adapters.in.scheduler.RefundExpirationScheduler}'s candidate query -
     * {@code domain.port.RefundRepository#findExpirationCandidates}'s real implementation. Native
     * SQL, same reasoning as {@code PaymentJpaRepository#findExpirationCandidates}: JPQL has no
     * clean way to join an unrelated entity's table by a foreign key that is not a mapped
     * association ({@link RefundEntity} deliberately has none to {@code MerchantConfigEntity}).
     *
     * <p>Unlike the payment version this joins straight from {@code refunds} to
     * {@code merchant_configs} - {@code refunds.merchant_id} is already a column on the entity
     * (unlike {@code PaymentEntity}, no join through {@code payments} is needed to learn which
     * merchant's window applies). {@code LEFT JOIN} + {@code COALESCE(..., 900)}: a merchant with
     * no {@code merchant_configs} row at all still gets the same 900s default
     * {@link com.example.psp.paymentapi.domain.model.MerchantConfig#DEFAULT_REFUND_EXPIRATION_SECONDS}
     * names as everyone else.
     *
     * <p><b>THE CAST.</b> {@code CAST(:now AS timestamptz)} is not decorative: without it Postgres
     * cannot infer a type for the bind parameter on the left of {@code param - interval}
     * (the payment-expiration sweep shipped broken on exactly this omission once already - see
     * {@code PaymentJpaRepository#findExpirationCandidates}'s own identical cast). {@code NOT
     * EXISTS (...)} is what makes this idempotent across ticks WITHOUT an updatable local status
     * column to guard on (unlike the payment sweep's {@code p.status IN ('CREATED','PENDING')}):
     * {@link Refund} never advances past {@code REQUESTED} (see that class's javadoc), so the only
     * way to know "has this already been settled" is to ask whether a terminal
     * {@code refund_status_history} row exists yet - COMPLETED/FAILED (the provider's own verdict,
     * always authoritative) or EXPIRED (this sweep's own prior verdict, so a candidate already
     * published as EXPIRED on an earlier tick is not republished as a fresh row - the deterministic
     * eventId in {@code application.ExpireRefundsUseCase} would collapse a republish into the same
     * row anyway, but skipping it here also means a re-tick does not even attempt the redundant
     * publish).
     */
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
