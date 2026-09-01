package com.example.psp.paymentapi.adapters.in.web;

import java.util.List;

/**
 * Wire contract returned by {@code GET /api/payments/{paymentId}/refunds/{refundId}/history}
 * (M23). Field name is load-bearing: {@code {"items": [...]}}, same wrapper convention as
 * {@link PaymentHistoryResponse}.
 */
public record RefundHistoryResponse(List<RefundHistoryItemResponse> items) {
}
