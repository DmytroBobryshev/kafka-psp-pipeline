package com.example.psp.paymentapi.adapters.in.web;

import java.util.List;

/**
 * Wire contract returned by {@code GET /api/payments} (M19, the transactions panel). Field names
 * are load-bearing: the UI is built directly against this exact shape -
 * {@code {"items": [...], "page": N, "size": N, "total": N}} - not just "a list of payments".
 */
public record PaymentPageResponse(List<PaymentResponse> items, int page, int size, long total) {
}
