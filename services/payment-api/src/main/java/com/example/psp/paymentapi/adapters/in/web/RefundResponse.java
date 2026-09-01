package com.example.psp.paymentapi.adapters.in.web;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record RefundResponse(
        UUID id, UUID paymentId, String merchantId, BigDecimal amount, String currency, String status, String reason, Instant createdAt) {
}
