package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.util.Optional;
import java.util.UUID;

/**
 * Outbound port for persisting payments. Implemented by {@code adapters/out/persistence} - the
 * domain depends only on this interface, never on JPA or any concrete storage technology
 * (ADR-0007).
 */
public interface PaymentRepository {

    Payment save(Payment payment);

    Optional<Payment> findById(UUID id);

    /**
     * M19: applies a status change as an absolute value, not a delta - {@code UPDATE ... SET
     * status = :status WHERE id = :paymentId}. That shape is what makes this idempotent by
     * construction (ADR-0006's "every retryable operation MUST be idempotent"): replaying the
     * same {@code payments.payment-status-changed.v1} record twice (redelivery after a crash
     * before this consumer's offset commit, or a manual DLQ-style replay elsewhere in the system)
     * sets the row to the same value both times, rather than accumulating like an increment
     * would. See {@code application.ApplyPaymentOutcomeUseCase} for the caller and
     * {@code adapters.in.kafka.PaymentStatusChangedMapper} for how the event's own status
     * vocabulary becomes a {@link PaymentStatus}.
     *
     * <p>A no-op, not a 404, if {@code paymentId} is unknown to this table - the event that
     * drives this call can only ever name a payment this service itself created (ADR-0002's
     * {@code aggregateId}), so an unknown id here would be a bug elsewhere in the system, not a
     * condition this method needs to report on.
     */
    void updateStatus(UUID paymentId, PaymentStatus status);

    /**
     * M19's transactions-panel query: every payment for {@code merchantId} (or every merchant, if
     * {@code null}) in {@code status} (or any status, if {@code null}), newest first. Backs
     * {@code GET /api/payments} - see {@code adapters.in.web.PaymentQueryController} for the
     * query-parameter contract (including the size/page clamping that happens before this method
     * is ever called) and {@code adapters.out.persistence.PaymentJpaRepository#search} for the
     * actual optional-filter SQL.
     *
     * @param merchantId filter, or {@code null} to match every merchant.
     * @param status     filter, or {@code null} to match every status.
     * @param page       zero-based page index (already clamped to {@code >= 0} by the caller).
     * @param size       page size (already clamped to {@code [1, 100]} by the caller).
     */
    PaymentPage search(String merchantId, PaymentStatus status, int page, int size);
}
