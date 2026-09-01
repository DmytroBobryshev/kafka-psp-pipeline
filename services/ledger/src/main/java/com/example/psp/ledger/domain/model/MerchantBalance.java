package com.example.psp.ledger.domain.model;

import java.time.Instant;
import java.util.Objects;

public record MerchantBalance(String merchantId, Money balance, long entryCount, Instant updatedAt) {

    public MerchantBalance {
        Objects.requireNonNull(merchantId, "merchantId must not be null");
        if (merchantId.isBlank()) {
            throw new IllegalArgumentException("merchantId must not be blank");
        }
        Objects.requireNonNull(balance, "balance must not be null");
        if (entryCount < 0) {
            throw new IllegalArgumentException("entryCount must not be negative, was " + entryCount);
        }
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
    }
}
