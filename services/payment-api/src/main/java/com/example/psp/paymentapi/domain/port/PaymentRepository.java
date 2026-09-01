package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.time.Instant;
import java.util.List;
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
     * M20: a NO-DOWNGRADE, conditional UPDATE for the one non-terminal status this method's
     * absolute-value sibling {@link #updateStatus} must never be used for - {@code UPDATE ... SET
     * status = 'PENDING' ... WHERE id = :paymentId AND status = 'CREATED'}. Unlike
     * {@code updateStatus}, applying the SAME status twice is not what makes THIS call safe under
     * redelivery - the {@code WHERE status = 'CREATED'} guard is: psp-connector publishes PENDING
     * once, up front, before its provider call ({@code KafkaPaymentStatusPublisher#publishPending}),
     * so a redelivery of that record can arrive AFTER this row has already moved on to
     * SUCCEEDED/FAILED (events are ordered per-merchant partition, but redelivery is a replay, not
     * a guarantee about when). Without the guard, a late-replayed PENDING would silently downgrade
     * a resolved payment back to a non-terminal state. With it, the UPDATE's row count is simply 0
     * once the row has left CREATED - the same "idempotent by construction, not by a special-cased
     * check" shape {@link #updateStatus}'s javadoc describes, just conditioned on the FROM state
     * instead of always firing.
     *
     * <p>A no-op (not an error) if {@code paymentId} is unknown to this table, or if the row is no
     * longer CREATED - same "the event can only ever name a payment this service itself created"
     * reasoning as {@link #updateStatus}.
     */
    void applyPendingStatus(UUID paymentId);

    /**
     * M22: a NO-DOWNGRADE, conditional UPDATE for {@code EXPIRED} - {@code UPDATE ... SET status
     * = 'EXPIRED' ... WHERE id = :paymentId AND status IN ('CREATED', 'PENDING')}. Same shape as
     * {@link #applyPendingStatus}, generalised from a single required FROM state to a set of two:
     * an EXPIRED outcome (published by {@code adapters.in.scheduler.PaymentExpirationScheduler})
     * must never downgrade a payment that has already resolved to SUCCEEDED/FAILED, nor
     * re-EXPIRE one that is already EXPIRED (the scheduler republishes the same deterministic
     * eventId on every tick until the row actually leaves CREATED/PENDING, so this guard is what
     * makes those republishes idempotent no-ops rather than redundant work).
     *
     * <p><b>The one status this guard deliberately does NOT protect against:</b> a
     * late-arriving terminal outcome from psp-connector (SUCCEEDED/DECLINED) still applies via
     * {@link #updateStatus}'s unconditional absolute UPDATE, even to a row already EXPIRED here -
     * the provider's own answer is authoritative over this service's own expiry guess, so it is
     * allowed to overwrite it. See {@code application.ApplyPaymentOutcomeUseCase#execute}.
     *
     * <p>A no-op (not an error) if {@code paymentId} is unknown to this table, or if the row has
     * already left CREATED/PENDING - same reasoning as {@link #applyPendingStatus}.
     */
    void applyExpiredStatus(UUID paymentId);

    /**
     * M22: candidates for {@code adapters.in.scheduler.PaymentExpirationScheduler}'s sweep - every
     * payment still {@code CREATED}/{@code PENDING} whose {@code createdAt} is older than its own
     * merchant's configured {@code paymentExpirationSeconds} window (or the 900s default, for a
     * merchant with no {@code merchant_configs} row at all - see the real adapter's javadoc for
     * the LEFT JOIN + COALESCE this implies).
     *
     * @param now the instant to measure the window against - passed in rather than read from the
     *     database's own clock so {@code application.ExpirePaymentsUseCase} stays testable with an
     *     injected {@link java.time.Clock} (same reasoning as ledger's own
     *     {@code SweepExpiredReservationsUseCase#execute}, which computes its cutoff in Java for
     *     the identical reason).
     */
    List<Payment> findExpirationCandidates(Instant now);

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
