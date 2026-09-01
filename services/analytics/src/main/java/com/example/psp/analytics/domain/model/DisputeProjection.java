package com.example.psp.analytics.domain.model;

import java.util.Objects;

public record DisputeProjection(
        String disputeId,
        String paymentId,
        String merchantId,
        String reason,
        long sizeBytes,
        String sha256,
        boolean claimChecked,
        String bucket,
        String objectKey) {

    public DisputeProjection {
        Objects.requireNonNull(disputeId, "disputeId must not be null");
        Objects.requireNonNull(paymentId, "paymentId must not be null");
        Objects.requireNonNull(merchantId, "merchantId must not be null");
        Objects.requireNonNull(sha256, "sha256 must not be null");
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be >= 0, was " + sizeBytes);
        }
    }
}
