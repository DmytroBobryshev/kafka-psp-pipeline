package com.example.psp.paymentapi.application;

import java.util.UUID;

/**
 * Application-layer input model for {@link OpenDisputeUseCase} (M13). Deliberately separate from
 * the web DTO in {@code adapters/in/web} - same pattern as {@link RequestRefundCommand}.
 * {@code documentBytes} is already decoded from the web layer's base64 wire form by the time it
 * reaches here (see {@code adapters.in.web.DisputeWebMapper}) - the application layer works in
 * raw bytes, never in a wire encoding.
 */
public record OpenDisputeCommand(UUID paymentId, String reason, byte[] documentBytes, String contentType) {
}
