package com.example.psp.ledger.domain.model;

/**
 * The refund saga's state, as the ledger sees it (M11, ADR-0008 rule 1 - each participant persists
 * its own local view; there is no shared saga table). Persisted one row per {@code refundId} in
 * {@code refund_saga_state} and exposed read-only via {@code GET /api/refunds/{refundId}}.
 *
 * <p>Five of these six values are the module brief's exact vocabulary
 * (REQUESTED / RESERVED / COMPLETED / FAILED / RELEASED, docs/PLAN.md M11). {@link
 * #NEEDS_MANUAL_REVIEW} is this implementation's addition, required by the documented late-
 * completion edge case (docs/diagrams/sequence-refund-saga.md: {@code RELEASED -> SETTLED} is
 * illegal - the money left the acquirer, but the ledger already un-reserved it, so the right move
 * is neither to silently apply the debit nor to silently drop the event) - see {@code
 * adapters.out.persistence.RefundWriteTransaction#settle} for exactly when this state is entered.
 *
 * <p>{@link #REQUESTED} is never itself written to {@code refund_saga_state} by this
 * implementation: the ledger resolves refunds.refund-requested.v1 to {@link #RESERVED} or
 * {@link #FAILED} synchronously, inside the one Postgres transaction that also records the
 * inbound-event dedup row, so no external reader could observe an intermediate REQUESTED row even
 * if one were written. REQUESTED is real and observable exactly once: payment-api's synchronous
 * {@code 202 Accepted} response to the original {@code POST}, before the ledger has necessarily
 * consumed anything. See services/ledger/README.md's M11 section for the full state machine and
 * why a literal REQUESTED row would add a write with no observer.
 */
public enum RefundSagaStatus {

    /** Produced by payment-api; not persisted here (see class javadoc). */
    REQUESTED,

    /** Funds reserved: a {@code refund_reservations} row exists and the balance was debited. */
    RESERVED,

    /** The provider executed the refund; the reservation became a permanent DEBIT entry. */
    COMPLETED,

    /** Never reserved - the merchant's balance could not cover the amount (ADR-0006 category B). */
    FAILED,

    /** Compensated: the reservation was released and the balance restored (decline or TTL). */
    RELEASED,

    /**
     * A {@code refunds.refund-completed.v1} arrived for a refund already {@link #RELEASED} (or,
     * pathologically, {@link #FAILED}) - money moved at the provider after this ledger had already
     * told the world the reservation was gone. Terminal; requires a human, not a retry.
     */
    NEEDS_MANUAL_REVIEW;

    /** Once in one of these, no further Kafka event changes this refund's state. */
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == RELEASED || this == NEEDS_MANUAL_REVIEW;
    }
}
