package com.example.psp.paymentapi.adapters.in.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.paymentapi.application.UpsertMerchantConfigCommand;
import com.example.psp.paymentapi.domain.model.MerchantConfig;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * M22: {@code MerchantConfigWebMapperImpl} has no framework dependency at construction time - same
 * "instantiate the generated impl directly, no Spring" precedent as
 * {@code adapters.in.kafka.PaymentStatusChangedMapperTest}. Exercises the one piece of hand-written
 * logic {@link MerchantConfigWebMapper} carries: resolving an absent (null)
 * {@code paymentExpirationSeconds} on the request to {@link MerchantConfig
 * #DEFAULT_PAYMENT_EXPIRATION_SECONDS}, and passing an explicit value through unchanged.
 */
class MerchantConfigWebMapperTest {

    private final MerchantConfigWebMapper mapper = new MerchantConfigWebMapperImpl();

    @Test
    void absentPaymentExpirationSecondsResolvesToTheDefault() {
        UpsertMerchantConfigRequest request = request(null);

        UpsertMerchantConfigCommand command = mapper.toCommand("acme", request);

        assertThat(command.paymentExpirationSeconds())
                .isEqualTo(MerchantConfig.DEFAULT_PAYMENT_EXPIRATION_SECONDS);
    }

    @Test
    void explicitPaymentExpirationSecondsPassesThroughUnchanged() {
        UpsertMerchantConfigRequest request = request(1800);

        UpsertMerchantConfigCommand command = mapper.toCommand("acme", request);

        assertThat(command.paymentExpirationSeconds()).isEqualTo(1800);
    }

    @Test
    void toResponseCarriesPaymentExpirationSecondsThrough() {
        MerchantConfig config =
                new MerchantConfig(
                        "acme",
                        "ACME Corp",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR"),
                        null,
                        1500,
                        1800);

        MerchantConfigResponse response = mapper.toResponse(config);

        assertThat(response.paymentExpirationSeconds()).isEqualTo(1800);
    }

    private static UpsertMerchantConfigRequest request(Integer paymentExpirationSeconds) {
        return new UpsertMerchantConfigRequest(
                "ACME Corp",
                MerchantStatus.ACTIVE,
                "EUR",
                List.of("EUR"),
                null,
                1500,
                paymentExpirationSeconds);
    }
}
