package com.example.psp.paymentapi.adapters.in.web;

import jakarta.validation.constraints.NotBlank;

/**
 * Wire contract for {@code POST /api/payments/{paymentId}/disputes} (M13). A record, per
 * PLAN.md, same shape as {@link RequestRefundRequest}.
 *
 * <p>JSON + base64, not multipart: every other write endpoint in this service is a JSON {@code
 * @RequestBody} record, and the whole system already carries an "envelope + domain fields" JSON
 * shape at every boundary. Multipart would be the more common real-world choice for a file
 * upload, but it would make this one endpoint's request parsing genuinely different from every
 * other controller in the codebase for a demo whose point is the size-based routing AFTER the
 * bytes are in hand, not the wire encoding they arrived in - see
 * services/payment-api/README.md's "M13: claim check, measured" section.
 */
public record OpenDisputeRequest(
        @NotBlank(message = "reason must not be blank") String reason,
        @NotBlank(message = "documentBase64 must not be blank") String documentBase64,
        @NotBlank(message = "contentType must not be blank") String contentType) {
}
