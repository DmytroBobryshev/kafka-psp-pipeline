package com.example.psp.paymentapi.adapters.in.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.paymentapi.application.UpsertMerchantConfigCommand;
import com.example.psp.paymentapi.domain.model.MerchantConfig;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import java.util.List;
import org.junit.jupiter.api.Test;

class MerchantConfigWebMapperTest {

    private final MerchantConfigWebMapper mapper = new MerchantConfigWebMapperImpl();

    @Test
    void absentPaymentExpirationSecondsResolvesToTheDefault() {
        UpsertMerchantConfigRequest request = request(null, null);

        UpsertMerchantConfigCommand command = mapper.toCommand("acme", request);

        assertThat(command.paymentExpirationSeconds())
                .isEqualTo(MerchantConfig.DEFAULT_PAYMENT_EXPIRATION_SECONDS);
    }

    @Test
    void explicitPaymentExpirationSecondsPassesThroughUnchanged() {
        UpsertMerchantConfigRequest request = request(1800, null);

        UpsertMerchantConfigCommand command = mapper.toCommand("acme", request);

        assertThat(command.paymentExpirationSeconds()).isEqualTo(1800);
    }

    // M24: the refund-path mirror of the two paymentExpirationSeconds resolution tests above.

    @Test
    void absentRefundExpirationSecondsResolvesToTheDefault() {
        UpsertMerchantConfigRequest request = request(null, null);

        UpsertMerchantConfigCommand command = mapper.toCommand("acme", request);

        assertThat(command.refundExpirationSeconds())
                .isEqualTo(MerchantConfig.DEFAULT_REFUND_EXPIRATION_SECONDS);
    }

    @Test
    void explicitRefundExpirationSecondsPassesThroughUnchanged() {
        UpsertMerchantConfigRequest request = request(null, 2400);

        UpsertMerchantConfigCommand command = mapper.toCommand("acme", request);

        assertThat(command.refundExpirationSeconds()).isEqualTo(2400);
    }

    @Test
    void toResponseCarriesPaymentAndRefundExpirationSecondsThrough() {
        MerchantConfig config =
                new MerchantConfig(
                        "acme",
                        "ACME Corp",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR"),
                        null,
                        1500,
                        1800,
                        2400);

        MerchantConfigResponse response = mapper.toResponse(config);

        assertThat(response.paymentExpirationSeconds()).isEqualTo(1800);
        assertThat(response.refundExpirationSeconds()).isEqualTo(2400);
    }

    private static UpsertMerchantConfigRequest request(
            Integer paymentExpirationSeconds, Integer refundExpirationSeconds) {
        return new UpsertMerchantConfigRequest(
                "ACME Corp",
                MerchantStatus.ACTIVE,
                "EUR",
                List.of("EUR"),
                null,
                1500,
                paymentExpirationSeconds,
                refundExpirationSeconds);
    }
}
