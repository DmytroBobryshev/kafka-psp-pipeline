package com.example.psp.analytics.application;

public record ProjectDisputeCommand(
        String disputeId,
        String paymentId,
        String merchantId,
        String reason,
        boolean claimChecked,
        byte[] inlineBytes,
        String bucket,
        String objectKey,
        long referenceSizeBytes) {
}
