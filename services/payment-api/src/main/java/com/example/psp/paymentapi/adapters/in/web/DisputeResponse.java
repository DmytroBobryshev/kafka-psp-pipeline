package com.example.psp.paymentapi.adapters.in.web;

import java.util.UUID;

public record DisputeResponse(
        UUID disputeId,
        UUID paymentId,
        String merchantId,
        long sizeBytes,
        boolean claimChecked,
        String bucket,
        String objectKey) {
}
