package com.example.psp.ledger.domain.model;

import java.util.Objects;

public record ReserveOutcome(Decision decision, MerchantBalance balanceAfter) {

    public ReserveOutcome {
        Objects.requireNonNull(decision, "decision must not be null");
        if (decision == Decision.RESERVED) {
            Objects.requireNonNull(balanceAfter, "balanceAfter must be present when RESERVED");
        }
    }

    public static ReserveOutcome reserved(MerchantBalance balanceAfter) {
        return new ReserveOutcome(Decision.RESERVED, balanceAfter);
    }

    public static ReserveOutcome insufficientBalance() {
        return new ReserveOutcome(Decision.INSUFFICIENT_BALANCE, null);
    }

    public static ReserveOutcome alreadyProcessed() {
        return new ReserveOutcome(Decision.ALREADY_PROCESSED, null);
    }

    public enum Decision {
        RESERVED,
        INSUFFICIENT_BALANCE,
        ALREADY_PROCESSED
    }
}
