package com.example.psp.analytics.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MerchantWindowMetricsTest {

    private static PaymentOutcome outcome(boolean declined, long latency, String name, Integer threshold) {
        return new PaymentOutcome("acme", declined, latency, name, threshold);
    }

    @Test
    void emptyWindowHasNoRatesRatherThanNaN() {
        MerchantWindowMetrics empty = MerchantWindowMetrics.empty();

        assertThat(empty.totalCount()).isZero();
        assertThat(empty.declineRate()).isZero();
        assertThat(empty.declineRateBps()).isZero();
        assertThat(empty.avgPipelineLatencyMillis()).isZero();
        assertThat(empty.declineRateAlert()).isFalse();
    }

    @Test
    void countersAccumulateAndRatesDerive() {
        MerchantWindowMetrics metrics =
                MerchantWindowMetrics.empty()
                        .plus(outcome(false, 100L, "ACME Corp", 1500))
                        .plus(outcome(true, 200L, "ACME Corp", 1500))
                        .plus(outcome(false, 300L, "ACME Corp", 1500));

        assertThat(metrics.totalCount()).isEqualTo(3L);
        assertThat(metrics.declinedCount()).isEqualTo(1L);
        assertThat(metrics.declineRate()).isEqualTo(1.0d / 3.0d);
        assertThat(metrics.declineRateBps()).isEqualTo(3333L);
        assertThat(metrics.avgPipelineLatencyMillis()).isEqualTo(200.0d);
    }

    @Test
    void aggregationIsAssociative() {
        PaymentOutcome a = outcome(false, 10L, "ACME Corp", 1500);
        PaymentOutcome b = outcome(true, 20L, "ACME Corp", 1500);
        PaymentOutcome c = outcome(false, 30L, "ACME Corp", 1500);

        MerchantWindowMetrics oneOrder = MerchantWindowMetrics.empty().plus(a).plus(b).plus(c);
        MerchantWindowMetrics otherOrder = MerchantWindowMetrics.empty().plus(c).plus(a).plus(b);

        assertThat(oneOrder).isEqualTo(otherOrder);
    }

    @Test
    void enrichmentSurvivesARecordWithNoJoinedConfig() {
        MerchantWindowMetrics metrics =
                MerchantWindowMetrics.empty()
                        .plus(outcome(false, 10L, "ACME Corp", 1500))
                        .plus(outcome(true, 10L, null, null));

        assertThat(metrics.merchantDisplayName()).isEqualTo("ACME Corp");
        assertThat(metrics.declineRateAlertThresholdBps()).isEqualTo(1500);
        assertThat(metrics.totalCount()).isEqualTo(2L);
    }

    @Test
    void alertFiresOnlyWithAConfiguredNonZeroThresholdThatIsMet() {
        // 50% declines, threshold 15% -> fires.
        assertThat(
                        MerchantWindowMetrics.empty()
                                .plus(outcome(true, 0L, "ACME Corp", 1500))
                                .plus(outcome(false, 0L, "ACME Corp", 1500))
                                .declineRateAlert())
                .isTrue();

        assertThat(
                        MerchantWindowMetrics.empty()
                                .plus(outcome(true, 0L, null, null))
                                .plus(outcome(false, 0L, null, null))
                                .declineRateAlert())
                .isFalse();

        // Threshold 0 means opted out, not "always alert".
        assertThat(
                        MerchantWindowMetrics.empty()
                                .plus(outcome(true, 0L, "ACME Corp", 0))
                                .declineRateAlert())
                .isFalse();
    }
}
