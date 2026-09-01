package com.example.psp.paymentapi.application;

import java.util.UUID;

public record DisputeOutcome(
        UUID disputeId,
        UUID paymentId,
        String merchantId,
        long sizeBytes,
        boolean claimChecked,
        String bucket,
        String objectKey) {
}
