package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.paymentapi.domain.model.MerchantConfig;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.port.MerchantConfigPublisher;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MerchantConfigUseCaseTest {

    private final RecordingPublisher publisher = new RecordingPublisher();
    private final MerchantConfigUseCase useCase = new MerchantConfigUseCase(publisher);

    @Test
    void upsertPublishesTheCompleteSnapshot() {
        UpsertMerchantConfigCommand command =
                new UpsertMerchantConfigCommand(
                        "acme",
                        "ACME Corp",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR", "USD"),
                        "https://acme.test/hook",
                        1500,
                        1800,
                        2400);

        MerchantConfig published = useCase.upsert(command);

        assertThat(publisher.upserts).containsExactly(published);
        assertThat(publisher.tombstones).isEmpty();
        assertThat(published.merchantId()).isEqualTo("acme");
        assertThat(published.declineRateAlertThresholdBps()).isEqualTo(1500);
        assertThat(published.paymentExpirationSeconds()).isEqualTo(1800);
        // M24: same field-copy roundtrip, for refundExpirationSeconds.
        assertThat(published.refundExpirationSeconds()).isEqualTo(2400);
    }

    @Test
    void upsertAppliesTheDefaultPaymentExpirationSecondsWhenTheCommandCarriesIt() {
        UpsertMerchantConfigCommand command =
                new UpsertMerchantConfigCommand(
                        "acme",
                        "ACME Corp",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR"),
                        null,
                        1500,
                        MerchantConfig.DEFAULT_PAYMENT_EXPIRATION_SECONDS,
                        MerchantConfig.DEFAULT_REFUND_EXPIRATION_SECONDS);

        MerchantConfig published = useCase.upsert(command);

        assertThat(published.paymentExpirationSeconds()).isEqualTo(900);
        assertThat(published.refundExpirationSeconds()).isEqualTo(900);
    }

    @Test
    void rejectsAPaymentExpirationSecondsBelowTheMinimum() {
        UpsertMerchantConfigCommand invalid =
                new UpsertMerchantConfigCommand(
                        "acme", "ACME Corp", MerchantStatus.ACTIVE, "EUR", List.of("EUR"), null, 1500, 29, 900);

        assertThatThrownBy(() -> useCase.upsert(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("paymentExpirationSeconds");

        assertThat(publisher.upserts).isEmpty();
    }

    @Test
    void rejectsAPaymentExpirationSecondsAboveTheMaximum() {
        UpsertMerchantConfigCommand invalid =
                new UpsertMerchantConfigCommand(
                        "acme",
                        "ACME Corp",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR"),
                        null,
                        1500,
                        86_401,
                        900);

        assertThatThrownBy(() -> useCase.upsert(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("paymentExpirationSeconds");

        assertThat(publisher.upserts).isEmpty();
    }

    @Test
    void acceptsThePaymentExpirationSecondsBoundaryValues() {
        // 30 and 86400 are inclusive bounds, not exclusive - both must succeed.
        UpsertMerchantConfigCommand atMinimum =
                new UpsertMerchantConfigCommand(
                        "acme", "ACME Corp", MerchantStatus.ACTIVE, "EUR", List.of("EUR"), null, 1500, 30, 900);
        UpsertMerchantConfigCommand atMaximum =
                new UpsertMerchantConfigCommand(
                        "acme",
                        "ACME Corp",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR"),
                        null,
                        1500,
                        86_400,
                        900);

        assertThat(useCase.upsert(atMinimum).paymentExpirationSeconds()).isEqualTo(30);
        assertThat(useCase.upsert(atMaximum).paymentExpirationSeconds()).isEqualTo(86_400);
    }

    @Test
    void rejectsARefundExpirationSecondsBelowTheMinimum() {
        UpsertMerchantConfigCommand invalid =
                new UpsertMerchantConfigCommand(
                        "acme", "ACME Corp", MerchantStatus.ACTIVE, "EUR", List.of("EUR"), null, 1500, 900, 29);

        assertThatThrownBy(() -> useCase.upsert(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refundExpirationSeconds");

        assertThat(publisher.upserts).isEmpty();
    }

    @Test
    void rejectsARefundExpirationSecondsAboveTheMaximum() {
        UpsertMerchantConfigCommand invalid =
                new UpsertMerchantConfigCommand(
                        "acme",
                        "ACME Corp",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR"),
                        null,
                        1500,
                        900,
                        86_401);

        assertThatThrownBy(() -> useCase.upsert(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refundExpirationSeconds");

        assertThat(publisher.upserts).isEmpty();
    }

    @Test
    void acceptsTheRefundExpirationSecondsBoundaryValues() {
        // 30 and 86400 are inclusive bounds, not exclusive - both must succeed.
        UpsertMerchantConfigCommand atMinimum =
                new UpsertMerchantConfigCommand(
                        "acme", "ACME Corp", MerchantStatus.ACTIVE, "EUR", List.of("EUR"), null, 1500, 900, 30);
        UpsertMerchantConfigCommand atMaximum =
                new UpsertMerchantConfigCommand(
                        "acme",
                        "ACME Corp",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR"),
                        null,
                        1500,
                        900,
                        86_400);

        assertThat(useCase.upsert(atMinimum).refundExpirationSeconds()).isEqualTo(30);
        assertThat(useCase.upsert(atMaximum).refundExpirationSeconds()).isEqualTo(86_400);
    }

    @Test
    void deletePublishesATombstoneAndNotAStatusUpdate() {
        useCase.delete("acme");

        assertThat(publisher.tombstones).containsExactly("acme");
        assertThat(publisher.upserts).isEmpty();
    }

    @Test
    void deleteIsIdempotent() {
        useCase.delete("acme");
        useCase.delete("acme");

        assertThat(publisher.tombstones).containsExactly("acme", "acme");
    }

    @Test
    void domainInvariantsAreEnforcedByTheUseCaseNotOnlyByTheWebDto() {
        UpsertMerchantConfigCommand invalid =
                new UpsertMerchantConfigCommand(
                        "acme", "ACME Corp", MerchantStatus.ACTIVE, "EUR", List.of("EUR"), null, 20_000, 900, 900);

        assertThatThrownBy(() -> useCase.upsert(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("declineRateAlertThresholdBps");

        assertThat(publisher.upserts).isEmpty();
    }

    @Test
    void rejectsAPayoutCurrencyThatIsNotInAllowedCurrencies() {
        UpsertMerchantConfigCommand invalid =
                new UpsertMerchantConfigCommand(
                        "acme",
                        "ACME Corp",
                        MerchantStatus.ACTIVE,
                        "GBP",
                        List.of("EUR", "USD"),
                        null,
                        1500,
                        900,
                        900);

        assertThatThrownBy(() -> useCase.upsert(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payoutCurrency")
                .hasMessageContaining("allowedCurrencies");

        assertThat(publisher.upserts).isEmpty();
    }

    private static final class RecordingPublisher implements MerchantConfigPublisher {

        private final List<MerchantConfig> upserts = new ArrayList<>();
        private final List<String> tombstones = new ArrayList<>();

        @Override
        public void publishConfigChanged(MerchantConfig config) {
            upserts.add(config);
        }

        @Override
        public void publishConfigDeleted(String merchantId) {
            tombstones.add(merchantId);
        }
    }
}
