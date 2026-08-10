package com.example.psp.pspconnector.adapters.out.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.pspconnector.adapters.out.http.SimulatedPaymentProviderAdapter.ForcedOutcome;
import com.example.psp.pspconnector.config.ProviderSimulationProperties;
import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Plain JUnit against the adapter directly - no Spring context. Exercises M5's "deliberate
 * duplicate emission" knob ({@code psp-connector.provider.duplicate-rate}): the property that
 * makes {@code ProcessPaymentRequestUseCase}'s idempotent-consumer logic actually have something
 * to catch, by making a repeat {@code authorize()} call for a paymentId already seen replay the
 * exact same {@code providerEventId} instead of minting a fresh one.
 */
class SimulatedPaymentProviderAdapterTest {

    private static final UUID PAYMENT_ID = UUID.randomUUID();
    private static final String MERCHANT_ID = "merchant-1";
    private static final Money AMOUNT = new Money(BigDecimal.TEN, "EUR");

    @Test
    void duplicateRateOfOneAlwaysReplaysThePreviousProviderEventIdForTheSamePayment() {
        SimulatedPaymentProviderAdapter adapter = new SimulatedPaymentProviderAdapter(properties(1.0));

        ProviderResult first = adapter.authorize(PAYMENT_ID, MERCHANT_ID, AMOUNT);
        ProviderResult second = adapter.authorize(PAYMENT_ID, MERCHANT_ID, AMOUNT);
        ProviderResult third = adapter.authorize(PAYMENT_ID, MERCHANT_ID, AMOUNT);

        assertThat(second.providerEventId()).isEqualTo(first.providerEventId());
        assertThat(third.providerEventId()).isEqualTo(first.providerEventId());
        assertThat(second.outcome()).isEqualTo(first.outcome());
    }

    @Test
    void duplicateRateOfZeroNeverReplaysEvenForARepeatedPayment() {
        SimulatedPaymentProviderAdapter adapter = new SimulatedPaymentProviderAdapter(properties(0.0));

        ProviderResult first = adapter.authorize(PAYMENT_ID, MERCHANT_ID, AMOUNT);
        ProviderResult second = adapter.authorize(PAYMENT_ID, MERCHANT_ID, AMOUNT);

        assertThat(second.providerEventId()).isNotEqualTo(first.providerEventId());
    }

    @Test
    void firstCallForAPaymentIsNeverADuplicateRegardlessOfDuplicateRate() {
        SimulatedPaymentProviderAdapter adapter = new SimulatedPaymentProviderAdapter(properties(1.0));

        // Nothing cached yet for this paymentId - duplicateRate=1.0 has nothing to replay.
        ProviderResult result = adapter.authorize(PAYMENT_ID, MERCHANT_ID, AMOUNT);

        assertThat(result.providerEventId()).isNotNull();
    }

    private static ProviderSimulationProperties properties(double duplicateRate) {
        return new ProviderSimulationProperties(0, 1, 0.0, 0.0, 0, ForcedOutcome.APPROVED, duplicateRate);
    }
}
