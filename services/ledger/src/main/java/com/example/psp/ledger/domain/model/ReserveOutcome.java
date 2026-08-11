package com.example.psp.ledger.domain.model;

import java.util.Objects;

/**
 * The outcome of {@code domain.port.RefundRepository#tryReserveOrFail} (M11). Three-way, not
 * boolean, because "not reserved" has two causes the caller must treat completely differently:
 *
 * <ul>
 *   <li>{@link Decision#INSUFFICIENT_BALANCE} - a real business decision (ADR-0006 category B):
 *       the ledger's own {@code refunds.refund-failed.v1} must be published.
 *   <li>{@link Decision#ALREADY_PROCESSED} - the inbound {@code refunds.refund-requested.v1}
 *       event was already handled (replay, or a concurrent-delivery race lost on the
 *       {@code refund_processed_events} unique constraint): a pure no-op, nothing is published
 *       again, matching M5/M7's check-first-and-constraint-race idempotency shape.
 * </ul>
 *
 * @param decision    what happened.
 * @param balanceAfter the merchant's balance after reserving - only present when
 *                     {@code decision == RESERVED}.
 */
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
