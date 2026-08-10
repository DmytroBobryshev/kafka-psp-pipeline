package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import java.util.UUID;

/**
 * Outbound port for the {@code payment_attempts} table - one row per authorization attempt, with
 * TWO independent unique constraints backing M5's two distinct idempotency levels (see
 * {@code application.ProcessPaymentRequestUseCase}'s class javadoc for why both are necessary and
 * why they must stay separate, and README.md's M5 section for the proven defect that made the
 * first constraint alone insufficient):
 *
 * <ul>
 *   <li>{@code uq_payment_attempts_inbound_event_id} ({@code db/migration/V2__add_inbound_event_id_to_payment_attempts.sql})
 *       - LEVEL 1, replay/consumer idempotency, keyed on the inbound message's own
 *       {@code EventEnvelope.eventId}.
 *   <li>{@code uq_payment_attempts_payment_provider_event} ({@code db/migration/V1__create_payment_attempts_table.sql})
 *       - LEVEL 2, duplicate provider callbacks, keyed {@code (payment_id, provider_event_id)}.
 * </ul>
 *
 * <p>Implemented by {@code adapters.out.persistence.PostgresAttemptLogRepository} (ADR-0005:
 * psp-connector owns its own Postgres database, no other service touches it).
 *
 * <p>All methods use domain-only types in their signature (no Spring/JPA leaking through this
 * port - ADR-0007), even though the real implementation catches a Spring Data exception
 * internally to satisfy {@link #tryRecord}'s contract.
 */
public interface AttemptLogRepository {

    /**
     * M5 LEVEL 1's idempotency check, called by {@code application.ProcessPaymentRequestUseCase}
     * BEFORE {@code PaymentProviderPort#authorize} - the inbound event's own id is known up
     * front, unlike {@code providerEventId}, which is why this check can (and must) run ahead of
     * the side effect it exists to prevent (re-authorizing/charging on every topic replay).
     */
    boolean existsByInboundEventId(UUID inboundEventId);

    /**
     * M5 LEVEL 2's idempotency check (unchanged from the original M5 shape), called by
     * {@code application.ProcessPaymentRequestUseCase} after the provider call returns (that's
     * when {@code providerEventId} first becomes known) and before recording a new attempt or
     * publishing a status event. Catches a failure {@link #existsByInboundEventId} cannot see: a
     * provider callback genuinely delivered twice for an attempt made only once - not a replayed
     * inbound message.
     */
    boolean existsByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId);

    /**
     * Attempts to insert a new attempt row. Returns {@code true} if the insert succeeded,
     * {@code false} if EITHER unique constraint described in this interface's javadoc rejected it
     * as a duplicate.
     *
     * <p>This is M5's race-safe path for both levels: the two check-first methods above are each
     * a check-then-act that is itself racy under concurrent consumers/threads processing the same
     * redelivery - both can pass their existence check before either has inserted. The database's
     * unique constraints are the final authority; losing that race is a completely normal outcome
     * and MUST be reported by return value, never by letting a constraint-violation exception
     * escape to the caller.
     */
    boolean tryRecord(PaymentAttempt attempt);
}
