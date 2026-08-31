package com.example.psp.analytics.domain.model;

import java.util.Objects;

/**
 * M13's proof of the claim-check round trip: the projection {@code application.
 * ProjectDisputeUseCase} builds after dereferencing (or, for an inline document, simply reading)
 * the bytes {@code disputes.dispute-opened.v1} referred to. {@code sha256} and {@code sizeBytes}
 * are computed from the ACTUAL bytes analytics ends up holding - for a claim-checked dispute that
 * means bytes fetched from MinIO, not bytes trusted from the event - which is what makes this
 * projection evidence that the reference dereferences to the real document, not just that the
 * event carried a plausible-looking reference.
 */
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
