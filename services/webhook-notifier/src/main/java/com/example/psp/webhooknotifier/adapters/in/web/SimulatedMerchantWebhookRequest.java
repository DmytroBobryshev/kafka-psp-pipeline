package com.example.psp.webhooknotifier.adapters.in.web;

import java.math.BigDecimal;

/**
 * Body {@code adapters.out.http.RestClientMerchantWebhookClient} POSTs to
 * {@link SimulatedMerchantController}. Deliberately a thin, standalone DTO rather than a reuse of
 * {@code domain.model.WebhookDeliveryCommand}: this is the wire contract of an HTTP endpoint that
 * plays the role of an EXTERNAL system in this simulation, so it gets its own boundary type like
 * any other inbound web DTO (ADR-0007) even though, today, both ends of the call live in this
 * one service.
 */
public record SimulatedMerchantWebhookRequest(
        String paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status,
        String declineReason,
        String eventType,
        String refundId) {}
