package com.example.psp.pspconnector.adapters.out.http;

import com.example.psp.pspconnector.config.ProviderSimulationProperties;
import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import com.example.psp.pspconnector.domain.model.RefundOutcome;
import com.example.psp.pspconnector.domain.model.RefundProviderResult;
import com.example.psp.pspconnector.domain.port.PaymentProviderPort;
import com.example.psp.pspconnector.domain.port.RefundProviderPort;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SimulatedPaymentProviderAdapter implements PaymentProviderPort, RefundProviderPort {

    private static final Logger log = LoggerFactory.getLogger(SimulatedPaymentProviderAdapter.class);

    private static final Map<Integer, ProviderOutcome> PAYMENT_MAGIC_ENDINGS =
            Map.of(13, ProviderOutcome.DECLINED, 66, ProviderOutcome.TIMEOUT);

    private static final Set<Integer> REFUND_MAGIC_DECLINE_ENDINGS = Set.of(1, 5, 13, 55, 65, 75);

    private final ProviderSimulationProperties properties;
    private final Map<UUID, ProviderResult> lastResultByPaymentId = new ConcurrentHashMap<>();

    public SimulatedPaymentProviderAdapter(ProviderSimulationProperties properties) {
        this.properties = properties;
    }

    @Override
    public ProviderResult authorize(UUID paymentId, String merchantId, Money amount) {
        ProviderResult previous = lastResultByPaymentId.get(paymentId);
        if (previous != null && shouldReplayDuplicate()) {
            log.info(
                    "Simulated provider replaying duplicate callback paymentId={} merchantId={} "
                            + "providerEventId={} outcome={} (duplicate-rate hit)",
                    paymentId,
                    merchantId,
                    previous.providerEventId(),
                    previous.outcome());
            return previous;
        }

        long latencyMs = resolveLatencyMs();
        sleep(latencyMs);

        ProviderOutcome outcome = resolveOutcome(amount);
        UUID providerEventId = UUID.randomUUID();
        ProviderResult result = new ProviderResult(providerEventId, outcome, latencyMs);
        lastResultByPaymentId.put(paymentId, result);

        log.info(
                "Provider call paymentId={} merchantId={} latencyMs={} outcome={} providerEventId={}",
                paymentId,
                merchantId,
                latencyMs,
                outcome,
                providerEventId);

        return result;
    }

    @Override
    public RefundProviderResult refund(UUID refundId, UUID paymentId, String merchantId, Money amount) {
        long latencyMs = resolveLatencyMs();
        sleep(latencyMs);

        RefundOutcome outcome = resolveRefundOutcome(amount);
        UUID providerReference = UUID.randomUUID();

        log.info(
                "Simulated provider refund call refundId={} paymentId={} merchantId={} latencyMs={} "
                        + "outcome={} providerReference={}",
                refundId,
                paymentId,
                merchantId,
                latencyMs,
                outcome,
                providerReference);

        return new RefundProviderResult(providerReference, outcome, latencyMs);
    }

    private RefundOutcome resolveRefundOutcome(Money amount) {
        if (properties.magicAmounts().enabled() && REFUND_MAGIC_DECLINE_ENDINGS.contains(lastTwoDigits(amount))) {
            return RefundOutcome.DECLINED;
        }
        if (properties.refundForcedOutcome() != RefundForcedOutcome.NONE) {
            return properties.refundForcedOutcome().toRefundOutcome();
        }
        double roll = ThreadLocalRandom.current().nextDouble();
        return roll < properties.refundDeclineRate() ? RefundOutcome.DECLINED : RefundOutcome.COMPLETED;
    }

    private boolean shouldReplayDuplicate() {
        return properties.duplicateRate() > 0
                && ThreadLocalRandom.current().nextDouble() < properties.duplicateRate();
    }

    private long resolveLatencyMs() {
        if (properties.forcedLatencyMs() > 0) {
            return properties.forcedLatencyMs();
        }
        return ThreadLocalRandom.current()
                .nextLong(properties.minLatencyMs(), properties.maxLatencyMs() + 1);
    }

    private ProviderOutcome resolveOutcome(Money amount) {
        if (properties.magicAmounts().enabled()) {
            ProviderOutcome magic = PAYMENT_MAGIC_ENDINGS.get(lastTwoDigits(amount));
            if (magic != null) {
                return magic;
            }
        }
        if (properties.forcedOutcome() != ForcedOutcome.NONE) {
            return properties.forcedOutcome().toProviderOutcome();
        }

        double roll = ThreadLocalRandom.current().nextDouble();
        if (roll < properties.timeoutRate()) {
            return ProviderOutcome.TIMEOUT;
        }
        if (roll < properties.timeoutRate() + properties.declineRate()) {
            return ProviderOutcome.DECLINED;
        }
        return ProviderOutcome.APPROVED;
    }

    private static int lastTwoDigits(Money amount) {
        long totalCents = amount.amount().setScale(2, RoundingMode.HALF_UP).unscaledValue().longValueExact();
        return (int) (totalCents % 100);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while simulating provider latency", e);
        }
    }

    public enum ForcedOutcome {
        NONE,
        APPROVED,
        DECLINED,
        TIMEOUT;

        ProviderOutcome toProviderOutcome() {
            return ProviderOutcome.valueOf(name());
        }
    }

    public enum RefundForcedOutcome {
        NONE,
        COMPLETED,
        DECLINED;

        RefundOutcome toRefundOutcome() {
            return RefundOutcome.valueOf(name());
        }
    }
}
