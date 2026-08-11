package com.example.psp.ledger.domain.model;

/**
 * The outcome of {@code domain.port.RefundRepository#tryRelease} (M11): the guarded
 * {@code RESERVED -> RELEASED} transition - the compensating transaction, and the heart of this
 * module (ADR-0008). Used by both the compensation path (triggered by
 * {@code refunds.refund-failed.v1}) and the TTL sweeper (triggered by nothing at all, an
 * internally-scheduled timeout check) - see {@code adapters.out.persistence.RefundWriteTransaction}.
 *
 * @param result       see {@link RefundTransitionResult}.
 * @param balanceAfter present only when {@code result == APPLIED}.
 */
public record ReleaseOutcome(RefundTransitionResult result, MerchantBalance balanceAfter) {

    public static ReleaseOutcome applied(MerchantBalance balanceAfter) {
        return new ReleaseOutcome(RefundTransitionResult.APPLIED, balanceAfter);
    }

    public static ReleaseOutcome of(RefundTransitionResult result) {
        return new ReleaseOutcome(result, null);
    }
}
