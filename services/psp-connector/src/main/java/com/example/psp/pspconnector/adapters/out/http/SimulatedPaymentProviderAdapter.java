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
 *
 * <h2>M11: also the refund path's simulated provider</h2>
 *
 * <p>{@link #refund} lives on this same class rather than a separate one - one component
 * simulating one external acquirer, the same way a real PSP's HTTP client would expose both
 * charge and refund operations behind one integration. It reuses the SAME latency knobs
 * ({@code minLatencyMs}/{@code maxLatencyMs}/{@code forcedLatencyMs}) as {@link #authorize}, but a
 * SEPARATE, two-way outcome vocabulary ({@link RefundForcedOutcome}, {@link RefundOutcome}) - see
 * services/psp-connector/README.md's M11 section for the forceable property the orchestrator uses
 * to drive the happy-path and compensation proofs deterministically, and for why a refund timeout
 * is not modelled (the module brief only needs the two decisive outcomes).
 *
 * <h2>Magic amounts ({@code psp-connector.provider.magic-amounts.enabled})</h2>
 *
 * <p>Mirrors the real-PSP-sandbox convention (Stripe: docs.stripe.com/testing) of letting the
 * request itself pick its outcome via the amount's trailing digits, checked in both
 * {@link #resolveOutcome} and {@link #resolveRefundOutcome} before {@code forcedOutcome}/{@code
 * refundForcedOutcome} and before the dice roll. See services/psp-connector/README.md's "Forcing
 * outcomes (amount endings)" section for the full table and the precedence rationale.
 */
@Component
public class SimulatedPaymentProviderAdapter implements PaymentProviderPort, RefundProviderPort {

    private static final Logger log = LoggerFactory.getLogger(SimulatedPaymentProviderAdapter.class);

    // Amount-ending -> outcome, keyed on the last two digits of the amount (cents). See this
    // class's "Magic amounts" javadoc section and README's "Forcing outcomes (amount endings)".
    private static final Map<Integer, ProviderOutcome> PAYMENT_MAGIC_ENDINGS =
            Map.of(13, ProviderOutcome.DECLINED, 66, ProviderOutcome.TIMEOUT);

    // .13 is this simulation's own; .01/.05/.55/.65/.75 are Stripe's real refund-decline endings
    // (docs.stripe.com/testing) - both accepted so the sandbox convention matches the real one.
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

    /**
     * M11: executes a refund. No duplicate-callback simulation and no attempt cache here - the
     * refund path's idempotency is M5 level 1 only, entirely psp-connector's own
     * {@code refund_attempts} table (see {@code domain.model.RefundAttempt}'s javadoc); this
     * adapter is called at most once per distinct inbound event because
     * {@code application.ExecuteRefundUseCase} checks that BEFORE calling here.
     */
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
        // Precedence: magic amount ending (request selects its own outcome) > refundForcedOutcome
        // (cluster-wide override for drills) > dice roll. See this class's "Magic amounts" javadoc.
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
        // forcedLatencyMs > 0 overrides the random range entirely - used by the M4 rebalance-storm
        // drill to force every call to take exactly as long as needed to blow max.poll.interval.ms.
        if (properties.forcedLatencyMs() > 0) {
            return properties.forcedLatencyMs();
        }
        return ThreadLocalRandom.current()
                .nextLong(properties.minLatencyMs(), properties.maxLatencyMs() + 1);
    }

    private ProviderOutcome resolveOutcome(Money amount) {
        // Precedence: magic amount ending (request selects its own outcome) > forcedOutcome
        // (cluster-wide override for drills) > dice roll. See this class's "Magic amounts" javadoc.
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

    // Last two digits of the amount, e.g. 10.13 -> 13. Scale is fixed to 2 first so unscaledValue()
    // is exactly "total cents" regardless of how many fractional digits the caller passed in.
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

    /**
     * {@code psp-connector.provider.refund-forced-outcome} vocabulary (M11) - THE property name
     * the M11 orchestrator uses to drive the refund saga's two deterministic proofs. {@code NONE}
     * means "use {@code refundDeclineRate}"; {@code COMPLETED}/{@code DECLINED} force every
     * {@code refund()} call to that outcome. Deliberately two-way, not three - see this class's
     * M11 javadoc section for why no refund timeout is modelled.
     */
    public enum RefundForcedOutcome {
        NONE,
        COMPLETED,
        DECLINED;

        RefundOutcome toRefundOutcome() {
            return RefundOutcome.valueOf(name());
        }
    }
}
