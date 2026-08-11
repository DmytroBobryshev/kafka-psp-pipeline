package com.example.psp.ledger.domain.model;

/**
 * The outcome of {@code domain.port.RefundRepository#trySettle} (M11): the guarded
 * {@code RESERVED -> COMPLETED} transition, plus the merchant balance to publish alongside the
 * resulting {@code ledger.ledger-entry-recorded.v1} entry (unchanged by settlement itself - see
 * {@code domain.model.LedgerEntry#debit}'s javadoc for why - but still required on the event so a
 * downstream consumer never has to query this service's database, ADR-0004/ADR-0005).
 *
 * @param result       see {@link RefundTransitionResult}.
 * @param balanceAfter present only when {@code result == APPLIED}.
 */
public record SettleOutcome(RefundTransitionResult result, MerchantBalance balanceAfter) {

    public static SettleOutcome applied(MerchantBalance balanceAfter) {
        return new SettleOutcome(RefundTransitionResult.APPLIED, balanceAfter);
    }

    public static SettleOutcome of(RefundTransitionResult result) {
        return new SettleOutcome(result, null);
    }
}
