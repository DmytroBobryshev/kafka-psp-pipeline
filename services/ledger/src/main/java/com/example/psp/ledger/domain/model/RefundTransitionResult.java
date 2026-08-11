package com.example.psp.ledger.domain.model;

/**
 * The outcome of attempting a guarded refund-saga state transition (M11). ADR-0008 rule 3:
 * "every step is an explicit state transition, and illegal transitions are rejected, not assumed
 * impossible" - this enum is what lets {@code application/} tell those cases apart and log/act on
 * each differently, instead of collapsing them all into a boolean.
 *
 * <p>Returned by {@code domain.port.RefundRepository#trySettle} and {@code #tryRelease}. Every
 * value below corresponds to a concrete branch documented in
 * {@code adapters.out.persistence.RefundWriteTransaction}'s class javadoc.
 */
public enum RefundTransitionResult {

    /** The guarded transition fired: the saga moved to the new state and its side effects ran. */
    APPLIED,

    /**
     * The saga was already in the target terminal state - a genuine replay of the same lifecycle
     * event (or a race lost to a concurrent attempt at the identical transition, e.g. the TTL
     * sweeper and a compensating {@code refunds.refund-failed.v1} both trying to release the same
     * reservation). Idempotent no-op: nothing is re-applied, nothing is re-published.
     */
    ALREADY_APPLIED,

    /**
     * The event's effect does not apply because the precondition it assumes was never true - the
     * clearest case is a {@code refunds.refund-failed.v1} compensation attempt for a refund that
     * was never reserved in the first place ({@link RefundSagaStatus#FAILED} at request time, the
     * ledger consuming its own insufficient-balance event back - see
     * services/ledger/README.md's M11 section on ADR-0008 rule 7). Distinguished from
     * {@link #ALREADY_APPLIED} because nothing was ever applied to begin with.
     */
    NOT_APPLICABLE,

    /**
     * The documented late-completion edge case: money moved at the provider (a
     * {@code refunds.refund-completed.v1} arrived) for a refund whose reservation this ledger had
     * already released - {@code RELEASED -> SETTLED} is explicitly illegal
     * (docs/diagrams/sequence-refund-saga.md). The saga is moved to
     * {@link RefundSagaStatus#NEEDS_MANUAL_REVIEW} rather than either applying the debit (which
     * would double-count against the restored balance) or silently dropping the event (which would
     * hide a real discrepancy between the acquirer and the ledger).
     */
    ESCALATED_MANUAL_REVIEW,

    /**
     * A transition that has no sanctioned handling and must not silently change any state - e.g. a
     * release attempted against an already-{@link RefundSagaStatus#COMPLETED} refund, which would
     * let a merchant's balance be restored for money that was already permanently settled. Logged
     * loudly; nothing is written beyond the inbound-event dedup row.
     */
    REJECTED_ILLEGAL
}
