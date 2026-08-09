package com.example.psp.pspconnector.domain.model;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Value object: an amount in a currency. Pure Java, no framework dependency - part of the
 * hexagon's {@code domain/} package (ADR-0007).
 *
 * <p>Deliberately duplicated from {@code payment-api}'s {@code Money} rather than shared: ADR-0005
 * forbids shared domain/entity classes across services (only event contracts live in
 * {@code libs/common-events}), so every service owns its own value objects even when they look
 * identical today. They are free to diverge later without touching another service.
 *
 * @param amount   non-negative amount.
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
