package com.example.psp.webhooknotifier.adapters.out.http;

import java.math.BigDecimal;

/**
 * Wire shape {@code RestClientMerchantWebhookClient} POSTs to the merchant. Field-identical to
 * {@code adapters.in.web.SimulatedMerchantWebhookRequest} today, deliberately NOT the same class:
 * {@code adapters.out} must never depend on {@code adapters.in} (ADR-0007, enforced by
 * {@code architecture.HexagonalArchitectureTest}) - true structurally even though, right now, both
 * ends of this HTTP call happen to live in the same service. A real merchant integration would
 * define its own contract here independently of whatever this codebase's simulated endpoint
 * expects.
 */
public record WebhookCallbackRequest(
        String paymentId, String merchantId, BigDecimal amount, String currency, String status, String declineReason) {}
