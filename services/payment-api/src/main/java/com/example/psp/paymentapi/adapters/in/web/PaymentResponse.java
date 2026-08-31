package com.example.psp.paymentapi.adapters.in.web;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Wire contract returned by the payment web adapter. Records for DTOs, per PLAN.md. */
public record PaymentResponse(
        UUID id,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        Instant createdAt,
        Instant statusUpdatedAt) {
}
