package com.example.psp.paymentapi.adapters.in.web;

import java.time.Instant;
import java.util.UUID;

public record ProviderStatusResponse(
        UUID paymentId,
        String merchantId,
        boolean found,
        String status,
        String providerReference,
        Instant checkedAt,
        long roundTripMillis) {
}
