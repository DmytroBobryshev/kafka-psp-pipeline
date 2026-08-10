package com.example.psp.ledger.domain.model;

/**
 * Which way a {@link LedgerEntry} moves the merchant's balance.
 *
 * <p>M7 only ever produces {@link #CREDIT} entries: the single inbound topic today is
 * {@code payments.payment-status-changed.v1}, and the only status that moves money is a successful
 * capture. {@link #DEBIT} exists because the refund saga (M11) consumes
 * {@code refunds.refund-completed.v1} into this same table and is the reason the balance may go
 * negative - see {@link Money}'s javadoc.
 */
public enum EntryDirection {

    /** Money into the merchant balance (a successful payment). Balance delta is {@code +amount}. */
    CREDIT,

    /** Money out of the merchant balance (a completed refund - M11). Delta is {@code -amount}. */
    DEBIT;

    /** Signed multiplier applied to the entry amount when the balance is updated. */
    public int sign() {
        return this == CREDIT ? 1 : -1;
    }
}
