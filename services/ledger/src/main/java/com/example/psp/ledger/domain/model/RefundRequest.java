package com.example.psp.ledger.domain.model;

import java.util.Objects;
import java.util.UUID;

/**
 * The minimal identity of one refund, as needed by {@code domain.port.RefundEventPublisher}'s
 * request-scoped publications ({@code refunds.funds-reserved.v1} and the insufficient-balance
 * branch of {@code refunds.refund-failed.v1}). A domain-only carrier so the port never has to
 * reference {@code application.ReserveRefundCommand} (ADR-0007: ports reference {@code domain/}
 * types only, the same rule {@code LedgerEntryPublisher} and {@code PaymentStatusPublisher} both
 * already follow).
 */
public record RefundRequest(UUID refundId, UUID paymentId, String merchantId, Money amount) {

    public RefundRequest {
        Objects.requireNonNull(refundId, "refundId must not be null");
        Objects.requireNonNull(paymentId, "paymentId must not be null");
        Objects.requireNonNull(merchantId, "merchantId must not be null");
        Objects.requireNonNull(amount, "amount must not be null");
    }
}
