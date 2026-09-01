package com.example.psp.paymentapi.domain.model;

import java.math.BigDecimal;
import java.util.Objects;

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
