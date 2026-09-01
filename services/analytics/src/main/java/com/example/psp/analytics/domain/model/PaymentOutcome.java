package com.example.psp.analytics.domain.model;

public record PaymentOutcome(
        String merchantId,
        boolean declined,
        long pipelineLatencyMillis,
        String merchantDisplayName,
        Integer declineRateAlertThresholdBps) {

    public boolean merchantConfigKnown() {
        return merchantDisplayName != null;
    }
}
