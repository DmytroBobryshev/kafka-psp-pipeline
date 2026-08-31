package com.example.psp.paymentapi.adapters.in.web;

import java.util.List;

/**
 * Wire contract returned by {@code GET /api/merchants}. Field names are load-bearing:
 * {@code {"items": [...], "page": N, "size": N, "total": N}} - the UI is built directly against
 * this exact shape, same convention as {@link PaymentPageResponse}.
 */
public record MerchantPageResponse(List<MerchantResponse> items, int page, int size, long total) {
}
