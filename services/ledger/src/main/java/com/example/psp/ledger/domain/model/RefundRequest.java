package com.example.psp.ledger.domain.model;

import java.util.Objects;
import java.util.UUID;

public record RefundRequest(UUID refundId, UUID paymentId, String merchantId, Money amount) {

    public RefundRequest {
        Objects.requireNonNull(refundId, "refundId must not be null");
        Objects.requireNonNull(paymentId, "paymentId must not be null");
        Objects.requireNonNull(merchantId, "merchantId must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
    }
}
