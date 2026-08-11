package com.example.psp.pspconnector.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.pspconnector.domain.exception.ProviderTimeoutException;
import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import com.example.psp.pspconnector.domain.port.AttemptLogRepository;
import com.example.psp.pspconnector.domain.port.PaymentProviderPort;
import com.example.psp.pspconnector.domain.port.PaymentStatusPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Plain JUnit against {@code application/} + {@code domain/} - no Spring context, no Kafka, no
 * database, same pattern as {@code payment-api}'s {@code CreatePaymentUseCaseTest}. Exercises the
 * ADR-0006 branch this module exists to demonstrate: TIMEOUT never publishes and always throws;
 * APPROVED/DECLINED always publish and never throw. Also exercises M5's TWO idempotency levels
 * (see {@link ProcessPaymentRequestUseCase}'s class javadoc):
 *
 * <ul>
 *   <li><b>Level 1</b> (the fix) - replaying the same inbound eventId must never call {@link
 *       PaymentProviderPort#authorize} a second time. That is the key assertion in {@link
 *       #replayingSameInboundEventIdAuthorizesExactlyOnceAndPublishesExactlyOnce()}: the original
 *       defect authorized every replayed payment twice, and a test that only checked "published
 *       once" would not have caught it, because the second authorize() call itself is the harm.
 *   <li><b>Level 2</b> (unchanged) - a duplicate {@code (paymentId, providerEventId)} provider
 *       callback for otherwise distinct inbound events.
 * </ul>
 *
 * Both levels are exercised via their check-first path and their race path (a lost {@code
 * tryRecord}), all of which must leave exactly the right number of publishes and never let an
 * exception escape.
 */
class ProcessPaymentRequestUseCaseTest {

    private static final UUID PAYMENT_ID = UUID.randomUUID();
    private static final String MERCHANT_ID = "merchant-1";
    private static final Money AMOUNT = new Money(BigDecimal.TEN, "EUR");

    @Test
    void approvedAttemptIsRecordedAndPublished() {
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher);

        useCase.execute(command());

        assertThat(attemptLog.recorded).hasSize(1);
        assertThat(attemptLog.recorded.get(0).getOutcome()).isEqualTo(ProviderOutcome.APPROVED);
        assertThat(publisher.publishedCount.get()).isEqualTo(1);
    }

    @Test
    void declinedAttemptIsRecordedAndPublishedAsBusinessOutcome() {
        FakeProvider provider = new FakeProvider(ProviderOutcome.DECLINED);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher);

        // ADR-0006 category B: a decline must never throw - it is the answer, not an error.
        useCase.execute(command());

        assertThat(attemptLog.recorded).hasSize(1);
        assertThat(attemptLog.recorded.get(0).getOutcome()).isEqualTo(ProviderOutcome.DECLINED);
        assertThat(publisher.publishedCount.get()).isEqualTo(1);
    }

    @Test
    void timeoutAttemptIsRecordedButNeverPublishedAndThrows() {
        FakeProvider provider = new FakeProvider(ProviderOutcome.TIMEOUT);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher);

        // ADR-0006 category A: retryable - propagates so the Kafka listener never acks.
        assertThatThrownBy(() -> useCase.execute(command())).isInstanceOf(ProviderTimeoutException.class);

        // Still recorded - "just record attempts", even the ones that time out.
        assertThat(attemptLog.recorded).hasSize(1);
        assertThat(attemptLog.recorded.get(0).getOutcome()).isEqualTo(ProviderOutcome.TIMEOUT);
        assertThat(publisher.publishedCount.get()).isZero();
    }

    @Test
    void replayingSameInboundEventIdAuthorizesExactlyOnceAndPublishesExactlyOnce() {
        // THE key M5 assertion: the original ("fake") implementation deduped only after calling
        // the provider, so replaying the same inbound event authorized/charged a second time
        // every single time - measured as 50 events replayed once -> 50 processed twice, 0 caught.
        // Level 1 must catch this BEFORE authorize() runs again, not just skip the bookkeeping
        // afterwards - so the assertion that matters most here is authorizeCallCount(), not just
        // the publish count.
        UUID inboundEventId = UUID.randomUUID();
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher, meterRegistry);
        ProcessPaymentRequestCommand command = command(inboundEventId);

        useCase.execute(command);
        useCase.execute(command); // the replay: same inbound event, redelivered

        assertThat(provider.authorizeCallCount()).isEqualTo(1);
        assertThat(attemptLog.recorded).hasSize(1);
        assertThat(publisher.publishedCount.get()).isEqualTo(1);
        assertThat(meterRegistry.counter("psp-connector.payment.attempts.processed").count())
                .isEqualTo(1.0);
        assertThat(
                        meterRegistry
                                .counter("psp-connector.payment.attempts.deduplicated", "reason", "replay")
                                .count())
                .isEqualTo(1.0);
        assertThat(
                        meterRegistry
                                .counter(
                                        "psp-connector.payment.attempts.deduplicated",
                                        "reason",
                                        "provider-callback")
                                .count())
                .isZero();
    }

    @Test
    void distinctInboundEventIdForSamePaymentStillProcesses() {
        // A genuinely different inbound message for the same payment (e.g. a legitimate retry)
        // must still be processed - level 1 only blocks a replay of the SAME inbound event, never
        // a different one, even for the same paymentId.
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher);

        useCase.execute(command(UUID.randomUUID()));
        useCase.execute(command(UUID.randomUUID()));

        assertThat(provider.authorizeCallCount()).isEqualTo(2);
        assertThat(attemptLog.recorded).hasSize(2);
        assertThat(publisher.publishedCount.get()).isEqualTo(2);
    }

    @Test
    void duplicateProviderEventIdIsDeduplicatedAndPublishesExactlyOnce() {
        // Level 2: same providerEventId on every call - simulates SimulatedPaymentProviderAdapter's
        // M5 duplicate-rate replaying its previous callback (see that class's javadoc) - but each
        // call is a DISTINCT inbound event (command() mints a fresh inboundEventId each time), so
        // level 1 correctly does not block any of them; only level 2 does.
        UUID providerEventId = UUID.randomUUID();
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED, providerEventId);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher, meterRegistry);

        useCase.execute(command());
        useCase.execute(command());
        useCase.execute(command());

        assertThat(attemptLog.recorded).hasSize(1);
        assertThat(publisher.publishedCount.get()).isEqualTo(1);
        assertThat(meterRegistry.counter("psp-connector.payment.attempts.processed").count())
                .isEqualTo(1.0);
        assertThat(
                        meterRegistry
                                .counter(
                                        "psp-connector.payment.attempts.deduplicated",
                                        "reason",
                                        "provider-callback")
                                .count())
                .isEqualTo(2.0);
        assertThat(
                        meterRegistry
                                .counter("psp-connector.payment.attempts.deduplicated", "reason", "replay")
                                .count())
                .isZero();
    }

    @Test
    void distinctProviderEventIdsForSamePaymentAreNotDeduplicated() {
        // A retry that legitimately reaches the provider again gets a NEW providerEventId - not
        // the same duplicate, and must be processed and published as new work every time.
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher);

        useCase.execute(command());
        useCase.execute(command());

        assertThat(attemptLog.recorded).hasSize(2);
        assertThat(publisher.publishedCount.get()).isEqualTo(2);
        Set<UUID> distinctProviderEventIds = new HashSet<>();
        attemptLog.recorded.forEach(a -> distinctProviderEventIds.add(a.getProviderEventId()));
        assertThat(distinctProviderEventIds).hasSize(2);
    }

    @Test
    void raceOnInboundEventInsertIsDeduplicatedAsReplayWithoutEscapingAnException() {
        // Level 1's race path: existsByInboundEventId reports "not seen yet" on the pre-check (as
        // if a concurrent redelivery of the same inbound event hasn't landed when the check
        // runs), but tryRecord - standing in for the uq_payment_attempts_inbound_event_id
        // constraint (V2 migration), the real authority - reports it lost that race. Must be
        // absorbed as a level-1 duplicate: no exception escapes, no publish happens, and the
        // reason is correctly attributed to "replay" via the resolveRaceReason re-check.
        UUID inboundEventId = UUID.randomUUID();
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED);
        InboundEventRaceAttemptLog attemptLog = new InboundEventRaceAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher, meterRegistry);

        assertThatCode(() -> useCase.execute(command(inboundEventId))).doesNotThrowAnyException();

        assertThat(publisher.publishedCount.get()).isZero();
        assertThat(attemptLog.tryRecordCalls.get()).isEqualTo(1);
        assertThat(
                        meterRegistry
                                .counter("psp-connector.payment.attempts.deduplicated", "reason", "replay")
                                .count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.counter("psp-connector.payment.attempts.processed").count())
                .isZero();
    }

    @Test
    void raceOnProviderEventInsertIsDeduplicatedAsProviderCallbackWithoutEscapingAnException() {
        // Level 2's race path (unchanged behaviour from the original M5 implementation):
        // existsByPaymentIdAndProviderEventId reports "not seen yet" (as if a concurrent insert
        // for the same key hasn't landed when the check runs), but tryRecord - standing in for
        // the DB unique constraint, the real authority - reports it lost that race. That must be
        // absorbed as a duplicate: no exception escapes, no publish happens, and the counters and
        // log line still fire, correctly attributed to "provider-callback" (see RacyAttemptLog's
        // javadoc: existsByInboundEventId always reports false, so resolveRaceReason cannot
        // mistake this for a level 1 race).
        UUID providerEventId = UUID.randomUUID();
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED, providerEventId);
        RacyAttemptLog attemptLog = new RacyAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher, meterRegistry);

        assertThatCode(() -> useCase.execute(command())).doesNotThrowAnyException();

        assertThat(publisher.publishedCount.get()).isZero();
        assertThat(attemptLog.tryRecordCalls.get()).isEqualTo(1);
        assertThat(
                        meterRegistry
                                .counter(
                                        "psp-connector.payment.attempts.deduplicated",
                                        "reason",
                                        "provider-callback")
                                .count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.counter("psp-connector.payment.attempts.processed").count())
                .isZero();
    }

    private static ProcessPaymentRequestUseCase useCase(
            PaymentProviderPort provider, AttemptLogRepository attemptLog, PaymentStatusPublisher publisher) {
        return useCase(provider, attemptLog, publisher, new SimpleMeterRegistry());
    }

    private static ProcessPaymentRequestUseCase useCase(
            PaymentProviderPort provider,
            AttemptLogRepository attemptLog,
            PaymentStatusPublisher publisher,
            MeterRegistry meterRegistry) {
        return new ProcessPaymentRequestUseCase(provider, attemptLog, publisher, meterRegistry);
    }

    private static ProcessPaymentRequestCommand command() {
        return command(UUID.randomUUID());
    }

    /** Same payload every time except {@code inboundEventId} - lets a test control replay vs distinct. */
    private static ProcessPaymentRequestCommand command(UUID inboundEventId) {
        return new ProcessPaymentRequestCommand(
                PAYMENT_ID, MERCHANT_ID, AMOUNT, inboundEventId, "trace-1", "corr-1");
    }

    private static final class FakeProvider implements PaymentProviderPort {
        private final ProviderOutcome outcome;
        private final UUID fixedProviderEventId;
        private final AtomicInteger authorizeCalls = new AtomicInteger();

        private FakeProvider(ProviderOutcome outcome) {
            this(outcome, null);
        }

        /** {@code fixedProviderEventId == null} means "mint a fresh one on every call". */
        private FakeProvider(ProviderOutcome outcome, UUID fixedProviderEventId) {
            this.outcome = outcome;
            this.fixedProviderEventId = fixedProviderEventId;
        }

        @Override
        public ProviderResult authorize(UUID paymentId, String merchantId, Money amount) {
            authorizeCalls.incrementAndGet();
            UUID providerEventId = fixedProviderEventId != null ? fixedProviderEventId : UUID.randomUUID();
            return new ProviderResult(providerEventId, outcome, 0L);
        }

        int authorizeCallCount() {
            return authorizeCalls.get();
        }
    }

    /**
     * In-memory stand-in for the real {@code payment_attempts} table, including BOTH of its
     * unique constraints - level 1's on {@code causationEventId} (which is what the persistence
     * mapper writes into {@code inbound_event_id}, see {@code PaymentAttemptPersistenceMapper})
     * and level 2's on {@code (paymentId, providerEventId)}.
     */
    private static final class RecordingAttemptLog implements AttemptLogRepository {
        private final List<PaymentAttempt> recorded = new ArrayList<>();

        @Override
        public boolean existsByInboundEventId(UUID inboundEventId) {
            return recorded.stream().anyMatch(a -> a.getCausationEventId().equals(inboundEventId));
        }

        @Override
        public boolean existsByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId) {
            return recorded.stream()
                    .anyMatch(
                            a ->
                                    a.getPaymentId().equals(paymentId)
                                            && a.getProviderEventId().equals(providerEventId));
        }

        @Override
        public boolean tryRecord(PaymentAttempt attempt) {
            boolean duplicate =
                    existsByInboundEventId(attempt.getCausationEventId())
                            || existsByPaymentIdAndProviderEventId(
                                    attempt.getPaymentId(), attempt.getProviderEventId());
            if (duplicate) {
                return false;
            }
            recorded.add(attempt);
            return true;
        }

        @Override
        public java.util.Optional<PaymentAttempt> findLatestByPaymentId(UUID paymentId) {
            return recorded.stream()
                    .filter(a -> a.getPaymentId().equals(paymentId))
                    .max(java.util.Comparator.comparing(PaymentAttempt::getProcessedAt));
        }
    }

    /**
     * Simulates the level-1 check-then-act race explicitly: {@code existsByInboundEventId}
     * reports "not seen" on its first call (the pre-check, before {@code authorize()}) but "seen"
     * on every call after that - specifically the follow-up re-check {@code
     * ProcessPaymentRequestUseCase#resolveRaceReason} makes once {@code tryRecord} (always {@code
     * false} here, standing in for the DB unique constraint) reports it lost the race - as if a
     * concurrent redelivery of the same inbound event won the insert in between.
     */
    private static final class InboundEventRaceAttemptLog implements AttemptLogRepository {
        private final AtomicInteger existsByInboundEventIdCalls = new AtomicInteger();
        private final AtomicInteger tryRecordCalls = new AtomicInteger();

        @Override
        public boolean existsByInboundEventId(UUID inboundEventId) {
            return existsByInboundEventIdCalls.incrementAndGet() > 1;
        }

        @Override
        public boolean existsByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId) {
            return false;
        }

        @Override
        public boolean tryRecord(PaymentAttempt attempt) {
            tryRecordCalls.incrementAndGet();
            return false;
        }

        @Override
        public java.util.Optional<PaymentAttempt> findLatestByPaymentId(UUID paymentId) {
            return java.util.Optional.empty();
        }
    }

    /**
     * Simulates the level-2 check-then-act race explicitly: {@code
     * existsByPaymentIdAndProviderEventId} always reports "not seen" (as if a concurrent insert
     * of the same key hasn't landed yet when the check runs), but {@code tryRecord} - standing in
     * for the DB unique constraint, the real authority - always reports it lost the race, exactly
     * like {@code PostgresAttemptLogRepository} catching {@code DataIntegrityViolationException}
     * and returning {@code false} instead of letting it propagate. {@code existsByInboundEventId}
     * always reports "not seen" too, so {@code resolveRaceReason}'s follow-up re-check correctly
     * attributes this race to level 2, not level 1.
     */
    private static final class RacyAttemptLog implements AttemptLogRepository {
        private final AtomicInteger tryRecordCalls = new AtomicInteger();

        @Override
        public boolean existsByInboundEventId(UUID inboundEventId) {
            return false;
        }

        @Override
        public boolean existsByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId) {
            return false;
        }

        @Override
        public boolean tryRecord(PaymentAttempt attempt) {
            tryRecordCalls.incrementAndGet();
            return false;
        }

        @Override
        public java.util.Optional<PaymentAttempt> findLatestByPaymentId(UUID paymentId) {
            return java.util.Optional.empty();
        }
    }

    private static final class RecordingPublisher implements PaymentStatusPublisher {
        private final AtomicInteger publishedCount = new AtomicInteger();

        @Override
        public void publishStatusChanged(PaymentAttempt attempt) {
            publishedCount.incrementAndGet();
        }
    }
}
