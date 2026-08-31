package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.RefundAttempt;
import java.util.Optional;
import java.util.UUID;

/**
 * Outbound port for the {@code refund_attempts} table - the refund-path counterpart of
 * {@link AttemptLogRepository}, one row per refund execution attempt, with ONE unique constraint
 * ({@code uq_refund_attempts_inbound_event_id}, {@code db/migration/V3__create_refund_attempts_table.sql}):
 * M5 level 1, replay/consumer idempotency, keyed on the inbound {@code refunds.funds-reserved.v1}
 * event's own {@code eventId}. See {@code domain.model.RefundAttempt}'s javadoc for why this module
 * does not also replicate level 2.
 *
 * <p>Same check-first-plus-constraint-race shape as {@link AttemptLogRepository} throughout this
 * codebase - all signatures use {@code domain/} types only (ADR-0007).
 */
public interface RefundAttemptLogRepository {

    /**
     * Check-first idempotency, called by {@code application.ExecuteRefundUseCase} BEFORE
     * {@link RefundProviderPort#refund} - the inbound event's id is known up front, unlike
     * {@code providerReference}, so this check can (and must) run ahead of the side effect it
     * exists to prevent (executing the same refund at the provider twice on every topic replay).
     */
    boolean existsByInboundEventId(UUID inboundEventId);

    /**
     * Attempts to insert a new attempt row. {@code true} if inserted, {@code false} if the unique
     * constraint rejected it as a duplicate - the race-safe path, reported by return value, never
     * by letting a constraint-violation exception escape (same contract as
     * {@link AttemptLogRepository#tryRecord}).
     */
    boolean tryRecord(RefundAttempt attempt);

    /**
     * The loaded-row counterpart of {@link #existsByInboundEventId}, added for the M19 drill 9
     * fix: a dedup hit must REPUBLISH the stored attempt's outcome event (same
     * {@code statusEventId}) instead of skipping it - the row is written before the publish is
     * broker-acknowledged, so its existence never proved the event exists. See
     * {@code application.ExecuteRefundUseCase}.
     */
    Optional<RefundAttempt> findByInboundEventId(UUID inboundEventId);
}
