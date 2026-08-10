package com.example.psp.ledger.domain.model;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Value object: an amount in a currency. Pure Java, no framework dependency - part of the
 * hexagon's {@code domain/} package (ADR-0007).
 *
 * <p>Deliberately duplicated from {@code psp-connector}'s and {@code payment-api}'s {@code Money}
 * rather than shared: ADR-0005 forbids shared domain/entity classes across services (only event
 * contracts live in {@code libs/common-events}), so every service owns its own value objects even
 * when they look identical today.
 *
 * <p>One difference from the other two services' copies, and it matters here: a ledger
 * <b>balance</b> may legitimately be negative (a merchant whose refunds exceed their captures in
 * M11), so unlike {@code psp-connector}'s {@code Money} this record does <b>not</b> reject a
 * negative amount. Individual ledger entry amounts are still validated as positive by
 * {@link LedgerEntry} - direction, not sign, is what says which way the money moved.
 *
 * @param amount   the amount; may be negative for a balance, never for a single entry.
 * @param currency ISO-4217 three-letter currency code, e.g. {@code "EUR"}.
 */
public record Money(BigDecimal amount, String currency) {

    public Money {
        Objects.requireNonNull(amount, "amount must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        if (currency.length() != 3) {
            throw new IllegalArgumentException(
                    "currency must be an ISO-4217 3-letter code, was '" + currency + "'");
        }
    }

    public static Money of(BigDecimal amount, String currency) {
        return new Money(amount, currency);
    }

    public static Money zero(String currency) {
        return new Money(BigDecimal.ZERO, currency);
    }
}
