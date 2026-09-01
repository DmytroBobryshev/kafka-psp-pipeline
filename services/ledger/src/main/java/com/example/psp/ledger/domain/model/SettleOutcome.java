package com.example.psp.ledger.domain.model;

public record SettleOutcome(RefundTransitionResult result, MerchantBalance balanceAfter) {

    public static SettleOutcome applied(MerchantBalance balanceAfter) {
        return new SettleOutcome(RefundTransitionResult.APPLIED, balanceAfter);
    }

    public static SettleOutcome of(RefundTransitionResult result) {
        return new SettleOutcome(result, null);
    }
}
