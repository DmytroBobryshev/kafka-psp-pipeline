package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Refund;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Outbound port for persisting refund requests (M11). Implemented by
 * {@code adapters/out/persistence} - the domain depends only on this interface, never on JPA or
 * any concrete storage technology (ADR-0007).
 */
public interface RefundRepository {

    Refund save(Refund refund);

    /**
     * Sum of amounts already requested for {@code paymentId} (across every prior refund request,
     * regardless of how the saga later resolved downstream - payment-api has no visibility into
     * that, see {@link Refund}'s javadoc). Used by {@code application.RequestRefundUseCase} as a
     * fast-fail bounds check against the payment's original amount; the ledger's balance
     * reservation remains the actual authority on whether a refund can be paid.
     */
    BigDecimal sumRequestedAmount(UUID paymentId);

    List<Refund> findByPaymentId(UUID paymentId);

    /**
     * The single-refund lookup {@code GET /api/payments/{paymentId}/refunds/{refundId}/history}
     * (M23) needs: {@code refundId} scoped to {@code paymentId} in one query, so a refund that
     * exists but belongs to a DIFFERENT payment answers empty, same as one that does not exist at
     * all - both are a 404 to the caller (see {@code application.PaymentQueryUseCase
     * #refundHistory}), and neither should be distinguishable by response shape.
     */
    Optional<Refund> findByIdAndPaymentId(UUID id, UUID paymentId);

    /**
     * M24: {@code adapters.in.scheduler.RefundExpirationScheduler}'s candidate query - the
     * refund-path mirror of {@code PaymentRepository#findExpirationCandidates}. A refund is a
     * candidate when it was created before {@code now} minus its merchant's configured
     * {@code refundExpirationSeconds} window AND no terminal {@code refund_status_history} row
     * (COMPLETED/FAILED/EXPIRED) has been recorded for it yet - see
     * {@code adapters.out.persistence.RefundJpaRepository}'s native query for the exact SQL.
     *
     * <p>Unlike the payment version, this never conditionally writes {@code refunds.*} itself
     * (there is nothing to write - {@link Refund}'s own status is always {@code REQUESTED}, never
     * advances). The only effect of a candidate found here is
     * {@code application.ExpireRefundsUseCase} publishing an {@code EXPIRED}
     * {@code refunds.refund-status-changed.v1} record, consumed back into
     * {@code refund_status_history} by this service's own existing
     * {@code adapters.in.kafka.RefundStatusChangedListener} - history-only, all the way through.
     */
    List<Refund> findExpirationCandidates(Instant now);
}
