package com.example.psp.paymentapi.adapters.in.web;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentResponse(
        UUID id,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        Instant createdAt,
        Instant statusUpdatedAt) {
}
