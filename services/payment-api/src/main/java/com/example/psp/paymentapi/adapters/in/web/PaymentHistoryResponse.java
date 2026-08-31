package com.example.psp.paymentapi.adapters.in.web;

import java.util.List;

/**
 * Wire contract returned by {@code GET /api/payments/{id}/history} (M20). Field name is
 * load-bearing: the UI is built directly against this exact shape - {@code {"items": [...]}} -
 * same {@code items}-wrapper convention {@link PaymentPageResponse} already established for
 * {@code GET /api/payments}, minus the pagination fields this endpoint has no use for (a
 * payment's status trail is a handful of rows, never a page-worthy list).
 */
public record PaymentHistoryResponse(List<PaymentHistoryItemResponse> items) {
}
