package com.example.psp.analytics.domain.model;

public record MerchantWindowMetrics(
        long totalCount,
        long declinedCount,
        long latencySumMillis,
        String merchantDisplayName,
        Integer declineRateAlertThresholdBps) {

    private static final MerchantWindowMetrics EMPTY = new MerchantWindowMetrics(0L, 0L, 0L, null, null);

    public static MerchantWindowMetrics empty() {
        return EMPTY;
    }

    public MerchantWindowMetrics plus(PaymentOutcome outcome) {
        return new MerchantWindowMetrics(
                totalCount + 1,
                declinedCount + (outcome.declined() ? 1 : 0),
                latencySumMillis + outcome.pipelineLatencyMillis(),
                outcome.merchantDisplayName() != null ? outcome.merchantDisplayName() : merchantDisplayName,
                outcome.declineRateAlertThresholdBps() != null
                        ? outcome.declineRateAlertThresholdBps()
                        : declineRateAlertThresholdBps);
    }

    public double declineRate() {
        return totalCount == 0 ? 0.0d : (double) declinedCount / (double) totalCount;
    }

    public long declineRateBps() {
        return totalCount == 0 ? 0L : Math.round(10_000.0d * declinedCount / totalCount);
    }

    public double avgPipelineLatencyMillis() {
        return totalCount == 0 ? 0.0d : (double) latencySumMillis / (double) totalCount;
    }

    public boolean declineRateAlert() {
        return declineRateAlertThresholdBps != null
                && declineRateAlertThresholdBps > 0
                && totalCount > 0
                && declineRateBps() >= declineRateAlertThresholdBps;
    }
}
