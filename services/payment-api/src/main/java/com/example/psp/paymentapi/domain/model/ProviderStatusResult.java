package com.example.psp.paymentapi.domain.model;

import java.time.Instant;
import java.util.UUID;

public record ProviderStatusResult(
        UUID paymentId,
        String merchantId,
        boolean found,
        String status,
        String providerReference,
        Instant checkedAt,
        long roundTripMillis) {
}
