package com.example.psp.pspconnector.adapters.out.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.pspconnector.adapters.out.http.SimulatedPaymentProviderAdapter.ForcedOutcome;
import com.example.psp.pspconnector.adapters.out.http.SimulatedPaymentProviderAdapter.RefundForcedOutcome;
import com.example.psp.pspconnector.config.ProviderSimulationProperties;
import com.example.psp.pspconnector.config.ProviderSimulationProperties.MagicAmounts;
import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import com.example.psp.pspconnector.domain.model.RefundOutcome;
import com.example.psp.pspconnector.domain.model.RefundProviderResult;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Plain JUnit against the adapter directly - no Spring context. Exercises M5's "deliberate
 * duplicate emission" knob ({@code psp-connector.provider.duplicate-rate}) and the amount-ending
 * ("magic amounts") overrides on both {@code authorize()} and {@code refund()} - see the class
 * javadoc's "Magic amounts" section and README's "Forcing outcomes (amount endings)".
 */
class SimulatedPaymentProviderAdapterTest {

    private static final UUID PAYMENT_ID = UUID.randomUUID();
    private static final UUID REFUND_ID = UUID.randomUUID();
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

    @Test
    void paymentAmountEndingInThirteenDeclinesEvenWhenForcedOutcomeIsApproved() {
        SimulatedPaymentProviderAdapter adapter =
                new SimulatedPaymentProviderAdapter(properties(ForcedOutcome.APPROVED, RefundForcedOutcome.NONE, true));

        ProviderResult result = adapter.authorize(PAYMENT_ID, MERCHANT_ID, amount("10.13"));

        assertThat(result.outcome()).isEqualTo(ProviderOutcome.DECLINED);
    }

    @Test
    void paymentAmountEndingInSixtySixTimesOutEvenWhenForcedOutcomeIsApproved() {
        SimulatedPaymentProviderAdapter adapter =
                new SimulatedPaymentProviderAdapter(properties(ForcedOutcome.APPROVED, RefundForcedOutcome.NONE, true));

        ProviderResult result = adapter.authorize(PAYMENT_ID, MERCHANT_ID, amount("10.66"));

        assertThat(result.outcome()).isEqualTo(ProviderOutcome.TIMEOUT);
    }

    @Test
    void paymentAmountEndingIsIgnoredWhenMagicAmountsDisabled() {
        SimulatedPaymentProviderAdapter adapter =
                new SimulatedPaymentProviderAdapter(properties(ForcedOutcome.APPROVED, RefundForcedOutcome.NONE, false));

        // Same .13 ending as above, but magic-amounts.enabled=false falls through to forcedOutcome.
        ProviderResult result = adapter.authorize(PAYMENT_ID, MERCHANT_ID, amount("10.13"));

        assertThat(result.outcome()).isEqualTo(ProviderOutcome.APPROVED);
    }

    @Test
    void paymentNonMagicAmountIsUnaffectedByMagicAmounts() {
        SimulatedPaymentProviderAdapter adapter =
                new SimulatedPaymentProviderAdapter(properties(ForcedOutcome.APPROVED, RefundForcedOutcome.NONE, true));

        ProviderResult result = adapter.authorize(PAYMENT_ID, MERCHANT_ID, amount("10.42"));

        assertThat(result.outcome()).isEqualTo(ProviderOutcome.APPROVED);
    }

    @Test
    void refundAmountEndingInThirteenDeclinesEvenWhenForcedOutcomeIsCompleted() {
        SimulatedPaymentProviderAdapter adapter =
                new SimulatedPaymentProviderAdapter(properties(ForcedOutcome.NONE, RefundForcedOutcome.COMPLETED, true));

        RefundProviderResult result = adapter.refund(REFUND_ID, PAYMENT_ID, MERCHANT_ID, amount("10.13"));

        assertThat(result.outcome()).isEqualTo(RefundOutcome.DECLINED);
    }

    @Test
    void refundStripeDeclineEndingsAllDeclineEvenWhenForcedOutcomeIsCompleted() {
        SimulatedPaymentProviderAdapter adapter =
                new SimulatedPaymentProviderAdapter(properties(ForcedOutcome.NONE, RefundForcedOutcome.COMPLETED, true));

        // docs.stripe.com/testing's refund-decline endings - see README's "Forcing outcomes
        // (amount endings)" section.
        for (String ending : List.of("10.01", "10.05", "10.55", "10.65", "10.75")) {
            RefundProviderResult result = adapter.refund(REFUND_ID, PAYMENT_ID, MERCHANT_ID, amount(ending));
            assertThat(result.outcome()).as("amount %s", ending).isEqualTo(RefundOutcome.DECLINED);
        }
    }

    @Test
    void refundAmountEndingIsIgnoredWhenMagicAmountsDisabled() {
        SimulatedPaymentProviderAdapter adapter =
                new SimulatedPaymentProviderAdapter(properties(ForcedOutcome.NONE, RefundForcedOutcome.COMPLETED, false));

        RefundProviderResult result = adapter.refund(REFUND_ID, PAYMENT_ID, MERCHANT_ID, amount("10.13"));

        assertThat(result.outcome()).isEqualTo(RefundOutcome.COMPLETED);
    }

    @Test
    void refundNonMagicAmountIsUnaffectedByMagicAmounts() {
        SimulatedPaymentProviderAdapter adapter =
                new SimulatedPaymentProviderAdapter(properties(ForcedOutcome.NONE, RefundForcedOutcome.COMPLETED, true));

        RefundProviderResult result = adapter.refund(REFUND_ID, PAYMENT_ID, MERCHANT_ID, amount("10.42"));

        assertThat(result.outcome()).isEqualTo(RefundOutcome.COMPLETED);
    }

    private static Money amount(String value) {
        return new Money(new BigDecimal(value), "EUR");
    }

    private static ProviderSimulationProperties properties(double duplicateRate) {
        return properties(ForcedOutcome.APPROVED, RefundForcedOutcome.NONE, true, duplicateRate);
    }

    private static ProviderSimulationProperties properties(
            ForcedOutcome forcedOutcome, RefundForcedOutcome refundForcedOutcome, boolean magicAmountsEnabled) {
        return properties(forcedOutcome, refundForcedOutcome, magicAmountsEnabled, 0.0);
    }

    private static ProviderSimulationProperties properties(
            ForcedOutcome forcedOutcome,
            RefundForcedOutcome refundForcedOutcome,
            boolean magicAmountsEnabled,
            double duplicateRate) {
        return new ProviderSimulationProperties(
                0,
                1,
                0.0,
                0.0,
                0,
                forcedOutcome,
                duplicateRate,
                0.0,
                refundForcedOutcome,
                new MagicAmounts(magicAmountsEnabled));
    }
}
