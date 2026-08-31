package com.example.psp.paymentapi.adapters.in.web;

import java.util.UUID;

/**
 * Wire contract for {@code POST /api/payments/{paymentId}/disputes}'s response body (M13).
 * {@code claimChecked}/{@code bucket}/{@code objectKey} are the visible proof of which path this
 * dispute took - see {@code application.DisputeOutcome}'s javadoc.
 */
public record DisputeResponse(
        UUID disputeId,
        UUID paymentId,
        String merchantId,
        long sizeBytes,
        boolean claimChecked,
        String bucket,
        String objectKey) {
}
