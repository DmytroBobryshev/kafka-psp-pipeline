package com.example.psp.pspconnector.application;

import com.example.psp.pspconnector.domain.exception.ProviderTimeoutException;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import com.example.psp.pspconnector.domain.port.AttemptLogRepository;
import com.example.psp.pspconnector.domain.port.PaymentProviderPort;
import com.example.psp.pspconnector.domain.port.PaymentStatusPublisher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The single use case: authorize a payment against the (simulated) provider, record the attempt,
 * and - unless the provider timed out or the attempt is a duplicate - publish the resulting
 * status change.
 *
 * <p>{@code application/} orchestrates ports and MAY use Spring annotations, but never imports an
 * adapter type directly (ADR-0007). Micrometer/SLF4J are cross-cutting observability concerns,
 * not adapter types, so injecting them here (M5) does not violate that rule.
 *
 * <h2>M5: two distinct levels of idempotency</h2>
 *
 * <p><b>The first M5 implementation was fake.</b> It deduped on {@code (paymentId,
 * providerEventId)} alone, checked <em>after</em> calling {@link PaymentProviderPort#authorize}.
 * That catches a duplicate <em>provider callback</em> - but {@code providerEventId} is minted
 * fresh by the provider on every call (see {@link ProviderResult}'s javadoc), so replaying the
 * same inbound {@code payments.payment-requested.v1} record (crash-restart, rebalance, or an
 * operator resetting the consumer group's offsets to earliest and replaying the whole topic)
 * produced a brand-new {@code providerEventId} every time - the dedup check never matched, and
 * the provider was re-authorized/charged on every single replayed record. Measured: 50 events on
 * a single-partition topic, replayed once -> 50 processed twice, 0 caught as duplicates, 100
 * {@code payment_attempts} rows / 100 distinct {@code provider_event_id} / 100 status events
 * published for 50 distinct payments. See README.md's M5 section for the full story.
 *
 * <p>The fix requires <b>two</b> distinct checks, for two distinct failure modes, kept separate
 * below on purpose - collapsing them back into one key is exactly how the original defect
 * happened.
 *
 * <h3>Level 1 - replay/consumer idempotency (the fix)</h3>
 *
 * <p>Keyed on the <b>inbound</b> {@code EventEnvelope.eventId} (carried through as {@link
 * ProcessPaymentRequestCommand#causationEventId()} - see that record's javadoc), because that id
 * is stable across replays, rebalances and offset resets: it is part of the message itself, not
 * minted per-call by anything downstream. Checked via {@link
 * AttemptLogRepository#existsByInboundEventId} <b>before</b> {@link PaymentProviderPort#authorize}
 * is ever called. That ordering is the entire point of this fix: it prevents the side effect
 * (authorizing/charging a card again on every topic replay), not just a second bookkeeping row.
 * Persisted in the {@code inbound_event_id} column (V2 migration, unique constraint).
 *
 * <h3>Level 2 - duplicate provider callback (unchanged, kept as-is)</h3>
 *
 * <p>Keyed {@code (paymentId, providerEventId)}, checked after {@code authorize()} returns
 * (that's the earliest point {@code providerEventId} exists - see {@link ProviderResult}'s
 * javadoc). This catches a <em>genuinely different</em> failure that level 1 cannot see: the
 * provider itself delivering the same callback twice for an attempt we ourselves only made once
 * (see {@code adapters.out.http.SimulatedPaymentProviderAdapter}'s {@code duplicate-rate}
 * simulation). A legitimate retry after a timeout is new work with a new {@code providerEventId}
 * and must still be processed - so this check cannot be replaced by, or collapsed into, level 1.
 *
 * <p>Both checks have the same two-path shape, for the same reason: each is a check-then-act and
 * is itself racy under concurrent consumers/threads.
 *
 * <ul>
 *   <li><b>Check-first</b> - the common case, and cheap: no wasted insert attempt.
 *   <li><b>Race path</b> ({@link AttemptLogRepository#tryRecord} returning {@code false}) - the
 *       {@code payment_attempts} unique constraints (V1 for level 2, V2 for level 1) are the
 *       actual authority. Losing this race must never surface an exception to the caller - it is
 *       a normal outcome of at-least-once delivery under concurrency, not an error. Which
 *       constraint was actually lost is re-derived with one cheap follow-up read (see {@link
 *       #resolveRaceReason}) purely so the counters below stay honest even on this rare path.
 * </ul>
 */
@Service
public class ProcessPaymentRequestUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessPaymentRequestUseCase.class);

    private final PaymentProviderPort paymentProvider;
    private final AttemptLogRepository attemptLogRepository;
    private final PaymentStatusPublisher statusPublisher;
    private final Counter processedCounter;
    private final Counter deduplicatedReplayCounter;
    private final Counter deduplicatedProviderCallbackCounter;

    public ProcessPaymentRequestUseCase(
            PaymentProviderPort paymentProvider,
            AttemptLogRepository attemptLogRepository,
            PaymentStatusPublisher statusPublisher,
            MeterRegistry meterRegistry) {
        this.paymentProvider = paymentProvider;
        this.attemptLogRepository = attemptLogRepository;
        this.statusPublisher = statusPublisher;
        this.processedCounter =
                Counter.builder("psp-connector.payment.attempts.processed")
                        .description(
                                "Payment attempts newly recorded and processed (not an M5 duplicate)")
                        .register(meterRegistry);
        this.deduplicatedReplayCounter =
                Counter.builder("psp-connector.payment.attempts.deduplicated")
                        .tag("reason", "replay")
                        .description(
                                "M5 level 1: inbound-event replays skipped BEFORE calling the provider - "
                                        + "the fix for the proven defect described in this class's javadoc. "
                                        + "Keyed on the inbound EventEnvelope.eventId, not anything the "
                                        + "provider mints.")
                        .register(meterRegistry);
        this.deduplicatedProviderCallbackCounter =
                Counter.builder("psp-connector.payment.attempts.deduplicated")
                        .tag("reason", "provider-callback")
                        .description(
                                "M5 level 2 (unchanged): duplicate (paymentId, providerEventId) provider "
                                        + "callbacks skipped - a different failure mode than level 1, which "
                                        + "cannot see it; see class javadoc")
                        .register(meterRegistry);
    }

    public void execute(ProcessPaymentRequestCommand command) {
        UUID inboundEventId = command.causationEventId();

        // LEVEL 1 (the fix): check BEFORE the side effect. If this exact inbound event has
        // already been fully processed, paymentProvider.authorize() must never run again for it -
        // that ordering is the entire point, see class javadoc.
        if (attemptLogRepository.existsByInboundEventId(inboundEventId)) {
            recordDuplicate(DedupReason.REPLAY, command, inboundEventId, null);
            return;
        }

        ProviderResult result =
                paymentProvider.authorize(command.paymentId(), command.merchantId(), command.amount());

        // LEVEL 2 (unchanged): a genuinely different failure - the provider redelivering the same
        // callback for an attempt we ourselves only made once. Level 1 above cannot see this: it
        // only knows about our own inbound message id, not what the provider chose to do with it.
        if (attemptLogRepository.existsByPaymentIdAndProviderEventId(
                command.paymentId(), result.providerEventId())) {
            recordDuplicate(
                    DedupReason.PROVIDER_CALLBACK, command, inboundEventId, result.providerEventId());
            return;
        }

        PaymentAttempt attempt =
                PaymentAttempt.from(
                        command.paymentId(),
                        command.merchantId(),
                        command.amount(),
                        result,
                        command.causationEventId(),
                        command.traceId(),
                        command.correlationId());

        boolean inserted = attemptLogRepository.tryRecord(attempt);
        if (!inserted) {
            // Race path: lost to a concurrent insert between one of the two check-first reads
            // above and this insert - see class javadoc.
            DedupReason reason = resolveRaceReason(inboundEventId);
            recordDuplicate(reason, command, inboundEventId, result.providerEventId());
            return;
        }

        processedCounter.increment();

        if (attempt.getOutcome() == ProviderOutcome.TIMEOUT) {
            // ADR-0006 category A (retryable). Deliberately NOT published, and this throw
            // propagates straight out of adapters.in.kafka.PaymentRequestedListener uncaught, so
            // Acknowledgment.acknowledge() is never reached for this record - see
            // config.KafkaConsumerConfig for what the container's error handler does next.
            throw new ProviderTimeoutException(command.paymentId());
        }

        // ADR-0006 category B: APPROVED and DECLINED are both business outcomes, not errors.
        // Both publish a status event and both let the listener ack normally afterwards - a
        // decline is the answer, not a failure, and must never be retried or DLQ'd.
        statusPublisher.publishStatusChanged(attempt);
    }

    /**
     * The race path (a lost {@link AttemptLogRepository#tryRecord}) only tells us <em>that</em> a
     * unique constraint was violated, not <em>which</em> of the two (V1 level 2 vs V2 level 1)
     * was the one that actually fired - {@code tryRecord}'s boolean contract is deliberately
     * identical for both. Re-reading {@code existsByInboundEventId} is one cheap extra lookup on
     * this rare path, and it is authoritative: whichever key just landed a row is the one that won
     * the race, so if it now exists, level 1 is what this attempt collided with.
     */
    private DedupReason resolveRaceReason(UUID inboundEventId) {
        return attemptLogRepository.existsByInboundEventId(inboundEventId)
                ? DedupReason.REPLAY
                : DedupReason.PROVIDER_CALLBACK;
    }

    private void recordDuplicate(
            DedupReason reason,
            ProcessPaymentRequestCommand command,
            UUID inboundEventId,
            UUID providerEventId) {
        switch (reason) {
            case REPLAY -> deduplicatedReplayCounter.increment();
            case PROVIDER_CALLBACK -> deduplicatedProviderCallbackCounter.increment();
        }
        log.info(
                "Deduplicated payment attempt reason={} paymentId={} inboundEventId={} "
                        + "providerEventId={} merchantId={} - already processed, skipping attempt-log "
                        + "write and publish, acknowledging normally",
                reason,
                command.paymentId(),
                inboundEventId,
                providerEventId,
                command.merchantId());
    }

    /**
     * M5's two dedup reasons (see class javadoc). Kept private and Micrometer-tag-only - not a
     * concept the rest of the domain needs to know about.
     */
    private enum DedupReason {
        REPLAY,
        PROVIDER_CALLBACK
    }
}
