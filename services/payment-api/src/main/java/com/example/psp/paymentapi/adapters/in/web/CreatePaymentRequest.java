package com.example.psp.paymentapi.adapters.in.web;

import java.math.BigDecimal;

/** Wire contract for {@code POST /api/payments}. Records for DTOs, per PLAN.md. */
public record CreatePaymentRequest(String merchantId, BigDecimal amount, String currency) {
}
