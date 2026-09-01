package com.example.psp.ledger.domain.model;

public record ReleaseOutcome(RefundTransitionResult result, MerchantBalance balanceAfter) {

    public static ReleaseOutcome applied(MerchantBalance balanceAfter) {
        return new ReleaseOutcome(RefundTransitionResult.APPLIED, balanceAfter);
    }

    public static ReleaseOutcome of(RefundTransitionResult result) {
        return new ReleaseOutcome(result, null);
    }
}
