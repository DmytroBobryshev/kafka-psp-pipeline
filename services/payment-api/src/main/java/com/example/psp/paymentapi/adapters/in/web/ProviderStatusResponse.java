package com.example.psp.paymentapi.adapters.in.web;

import java.time.Instant;
import java.util.UUID;

/**
 * Wire contract returned by {@link ProviderStatusController} (M12). Records for DTOs, per
 * PLAN.md. {@code roundTripMillis} is deliberately part of the contract - the whole point of this
 * endpoint is to make the request-reply round trip OBSERVABLE end to end, not just functional.
 */
public record ProviderStatusResponse(
        UUID paymentId,
        String merchantId,
        boolean found,
        String status,
        String providerReference,
        Instant checkedAt,
        long roundTripMillis) {
}
