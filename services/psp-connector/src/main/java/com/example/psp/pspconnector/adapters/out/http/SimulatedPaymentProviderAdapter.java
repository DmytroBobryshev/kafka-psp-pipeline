package com.example.psp.pspconnector.adapters.out.http;

import com.example.psp.pspconnector.config.ProviderSimulationProperties;
import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import com.example.psp.pspconnector.domain.port.PaymentProviderPort;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Simulated acquirer/PSP (M4, duplicate emission added in M5). Lives under
 * {@code adapters.out.http} - not because it opens a socket today, but because it is the
 * ADR-0004 carve-out for outbound HTTP that leaves the system, and a real implementation calling
 * a real provider over {@code WebClient} would live behind this exact port with zero change to
 * {@code application/} or {@code domain/}.
 *
 * <p>Every knob here is read from {@link ProviderSimulationProperties}
 * ({@code psp-connector.provider.*} in {@code application.yml}), specifically so the "prove it"
 * experiments in the README can force deterministic outcomes (a fixed latency for the
 * rebalance-storm drill, a forced decline/timeout rate, etc.) without touching code.
 *
 * <h2>M5: deliberate duplicate emission ({@code psp-connector.provider.duplicate-rate})</h2>
 *
 * <p>This adapter remembers the last {@link ProviderResult} it returned for each
 * {@code paymentId} ({@link #lastResultByPaymentId}). When {@code authorize()} is called again
 * for a {@code paymentId} already in that map, {@code duplicateRate} is the probability of
 * replaying that exact same result (same {@code providerEventId}) instead of minting a fresh
 * one - simulating a real acquirer that dedupes/replays its own callback for the same logical
 * attempt (e.g. keyed on an idempotency key) rather than treating a redelivered request as brand
 * new work. This is what turns an ordinary Kafka redelivery of
 * {@code payments.payment-requested.v1} (crash-and-restart, manual offset reset/replay, a future
 * M8 retry) into a genuine {@code (paymentId, providerEventId)} collision that the M5 idempotent
 * consumer ({@code application.ProcessPaymentRequestUseCase}) can actually catch. Default 0 keeps
 * M4's original behaviour: every call, even a redelivery, mints a brand-new
 * {@code providerEventId} - the "known defect" the M4 README section describes.
 *
 * <p><b>Compromise:</b> {@link #lastResultByPaymentId} is an unbounded, in-memory,
 * single-instance cache - acceptable for this learning exercise's scale and lifetime, but not a
 * production pattern (no eviction, and it is invisible to other psp-connector instances in the
 * consumer group). A real implementation would use a bounded/TTL cache or push the idempotency
 * key to the actual provider API.
 */
@Component
public class SimulatedPaymentProviderAdapter implements PaymentProviderPort {

    private static final Logger log = LoggerFactory.getLogger(SimulatedPaymentProviderAdapter.class);

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

        ProviderOutcome outcome = resolveOutcome();
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

    private boolean shouldReplayDuplicate() {
        return properties.duplicateRate() > 0
                && ThreadLocalRandom.current().nextDouble() < properties.duplicateRate();
    }

    private long resolveLatencyMs() {
        // forcedLatencyMs > 0 overrides the random range entirely - used by the M4 rebalance-storm
        // drill to force every call to take exactly as long as needed to blow max.poll.interval.ms.
        if (properties.forcedLatencyMs() > 0) {
            return properties.forcedLatencyMs();
        }
        return ThreadLocalRandom.current()
                .nextLong(properties.minLatencyMs(), properties.maxLatencyMs() + 1);
    }

    private ProviderOutcome resolveOutcome() {
        // forcedOutcome overrides the dice roll entirely - used by experiments/tests that need a
        // deterministic result instead of a probabilistic one.
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

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while simulating provider latency", e);
        }
    }

    /**
     * {@code psp-connector.provider.forced-outcome} vocabulary. {@code NONE} means "use the
     * configured rates"; the other three force every call to that outcome, used by experiments
     * (e.g. a run with {@code forced-outcome: DECLINED} produces only business-outcome events,
     * never a provider call, to isolate the decline path from the random simulation).
     */
    public enum ForcedOutcome {
        NONE,
        APPROVED,
        DECLINED,
        TIMEOUT;

        ProviderOutcome toProviderOutcome() {
            return ProviderOutcome.valueOf(name());
        }
    }
}
