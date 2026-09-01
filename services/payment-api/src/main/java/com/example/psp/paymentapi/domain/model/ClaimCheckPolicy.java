package com.example.psp.paymentapi.domain.model;

public final class ClaimCheckPolicy {

    private ClaimCheckPolicy() {}

    public static boolean requiresClaimCheck(long sizeBytes, long thresholdBytes) {
        return sizeBytes > thresholdBytes;
    }
}
