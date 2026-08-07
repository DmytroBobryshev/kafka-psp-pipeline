package com.example.psp.paymentapi.domain.model;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Value object: an amount in a currency. Pure Java, no framework dependency - part of the
 * hexagon's {@code domain/} package (ADR-0007).
 *
 * <p>A record is the right shape here: {@code Money} has no identity, is fully defined by its
 * components, and its equality is naturally value-based.
 *
 * @param amount   non-negative amount. Scale/rounding rules per currency arrive with real
 *                 persistence in M3; for M1 the only invariant enforced is non-negativity.
 * @param currency ISO-4217 three-letter currency code, e.g. {@code "EUR"}.
 */
public record Money(BigDecimal amount, String currency) {

    public Money {
        Objects.requireNonNull(amount, "amount must not be null");
        Objects.requireNonNull(currency, "currency must not be null");
        if (amount.signum() < 0) {
            throw new IllegalArgumentException("amount must not be negative, was " + amount);
        }
        if (currency.length() != 3) {
            throw new IllegalArgumentException(
                    "currency must be an ISO-4217 3-letter code, was '" + currency + "'");
        }
    }

    public static Money of(BigDecimal amount, String currency) {
        return new Money(amount, currency);
    }
}
