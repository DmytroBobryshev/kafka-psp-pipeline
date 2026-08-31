package com.example.psp.pspconnector.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.common.events.UuidV7;
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
import java.util.Optional;
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
 *       PaymentProviderPort#authorize} a second time. The original defect authorized every
 *       replayed payment twice; the second authorize() call itself is the harm, so the assertion
 *       that matters most is authorizeCallCount().
 *   <li><b>Level 2</b> (unchanged) - a duplicate {@code (paymentId, providerEventId)} provider
 *       callback for otherwise distinct inbound events.
 * </ul>
 *
 * <p>Since the M19 drill 9 fix, a dedup hit REPUBLISHES the stored attempt's status event instead
 * of skipping it (the attempt row is written before the publish is broker-acknowledged, so its
 * existence never proved the event exists). The publish invariant is therefore no longer "exactly
 * one publish" but <b>"at-least-one publish, all carrying the SAME statusEventId"</b> - the
 * downstream idempotency key is what makes the extra publishes safe. Both dedup levels are still
 * exercised via their check-first path and their race path (a lost {@code tryRecord}), and {@link
 * #crashBetweenRecordAndPublishIsRepairedByRedelivery()} is the drill's loss scenario itself.
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
        assertThat(attemptLog.recorded.get(0).getStatusEventId()).isNotNull();
        assertThat(publisher.published).hasSize(1);
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
        assertThat(publisher.published).hasSize(1);
    }

    @Test
    void timeoutAttemptIsRecordedButNeverPublishedAndThrows() {
        FakeProvider provider = new FakeProvider(ProviderOutcome.TIMEOUT);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher);
        ProcessPaymentRequestCommand command = command();

        // ADR-0006 category A: retryable - propagates so the Kafka listener never acks.
        assertThatThrownBy(() -> useCase.execute(command)).isInstanceOf(ProviderTimeoutException.class);

        // Still recorded - "just record attempts", even the ones that time out.
        assertThat(attemptLog.recorded).hasSize(1);
        assertThat(attemptLog.recorded.get(0).getOutcome()).isEqualTo(ProviderOutcome.TIMEOUT);
        assertThat(publisher.published).isEmpty();

        // The redelivery of that same event: level 1 catches it, and the republish rule still
        // never publishes a TIMEOUT row (there is no status event for a timeout, by design).
        assertThatCode(() -> useCase.execute(command)).doesNotThrowAnyException();
        assertThat(publisher.published).isEmpty();
    }

    @Test
    void replayingSameInboundEventIdAuthorizesExactlyOnceAndRepublishesSameEventId() {
        // THE key M5 assertion: replaying the same inbound event must not authorize/charge a
        // second time. Since the M19 drill 9 fix the replay DOES publish again - deliberately -
        // but under the stored statusEventId, so downstream dedup sees one logical event.
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
        assertThat(publisher.published).hasSize(2);
        assertThat(publisher.distinctStatusEventIds()).hasSize(1);
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
    void crashBetweenRecordAndPublishIsRepairedByRedelivery() {
        // M19 drill 9's exact loss scenario, at use-case granularity. First delivery: the attempt
        // row lands, then the publish fails (standing in for "the pod died before the broker
        // acknowledged the send" - with the blocking publisher, that failure now reaches the
        // listener, so the offset is never committed). Redelivery: level 1 finds the row, skips
        // the provider, and republishes THE SAME statusEventId. Before the fix, the redelivery
        // was "deduplicated and skipped" and the event was gone forever.
        UUID inboundEventId = UUID.randomUUID();
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher(1); // first publish attempt fails
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher);
        ProcessPaymentRequestCommand command = command(inboundEventId);

        // First delivery: the publish failure must escape so the listener never acks.
        assertThatThrownBy(() -> useCase.execute(command)).isInstanceOf(RuntimeException.class);
        assertThat(attemptLog.recorded).hasSize(1);

        // The redelivery repairs it - no second charge, same logical event.
        assertThatCode(() -> useCase.execute(command)).doesNotThrowAnyException();
        assertThat(provider.authorizeCallCount()).isEqualTo(1);
        assertThat(publisher.published).hasSize(2); // the failed attempt + the successful republish
        assertThat(publisher.distinctStatusEventIds()).hasSize(1);
        assertThat(publisher.published.get(1).getStatusEventId())
                .isEqualTo(attemptLog.recorded.get(0).getStatusEventId());
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
        assertThat(publisher.published).hasSize(2);
        assertThat(publisher.distinctStatusEventIds()).hasSize(2);
    }

    @Test
    void duplicateProviderEventIdIsDeduplicatedAndRepublishesSameEventId() {
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
        assertThat(publisher.published).hasSize(3);
        assertThat(publisher.distinctStatusEventIds()).hasSize(1);
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
        assertThat(publisher.published).hasSize(2);
        Set<UUID> distinctProviderEventIds = new HashSet<>();
        attemptLog.recorded.forEach(a -> distinctProviderEventIds.add(a.getProviderEventId()));
        assertThat(distinctProviderEventIds).hasSize(2);
    }

    @Test
    void raceOnInboundEventInsertIsDeduplicatedAsReplayAndRepublishesTheWinner() {
        // Level 1's race path: findByInboundEventId reports "not seen yet" on the pre-check (as
        // if a concurrent redelivery of the same inbound event hasn't landed when the check
        // runs), but tryRecord - standing in for the uq_payment_attempts_inbound_event_id
        // constraint (V2 migration), the real authority - reports it lost that race. Must be
        // absorbed as a level-1 duplicate: no exception escapes, the reason is attributed to
        // "replay" via the follow-up re-read, and the WINNER's row (not the losing attempt) is
        // what gets republished.
        UUID inboundEventId = UUID.randomUUID();
        PaymentAttempt winner = approvedAttempt(inboundEventId, UUID.randomUUID());
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED);
        InboundEventRaceAttemptLog attemptLog = new InboundEventRaceAttemptLog(winner);
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher, meterRegistry);

        assertThatCode(() -> useCase.execute(command(inboundEventId))).doesNotThrowAnyException();

        assertThat(attemptLog.tryRecordCalls.get()).isEqualTo(1);
        assertThat(publisher.published).hasSize(1);
        assertThat(publisher.published.get(0).getStatusEventId()).isEqualTo(winner.getStatusEventId());
        assertThat(
                        meterRegistry
                                .counter("psp-connector.payment.attempts.deduplicated", "reason", "replay")
                                .count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.counter("psp-connector.payment.attempts.processed").count())
                .isZero();
    }

    @Test
    void raceOnProviderEventInsertIsDeduplicatedAsProviderCallbackAndRepublishesTheWinner() {
        // Level 2's race path: findByPaymentIdAndProviderEventId reports "not seen yet" on the
        // pre-check, but tryRecord - standing in for the DB unique constraint, the real authority
        // - reports it lost that race, exactly like PostgresAttemptLogRepository catching
        // DataIntegrityViolationException and returning false. findByInboundEventId stays empty
        // throughout, so the follow-up re-read correctly attributes this race to level 2, and the
        // winner row is looked up by (paymentId, providerEventId) instead.
        UUID providerEventId = UUID.randomUUID();
        PaymentAttempt winner = approvedAttempt(UUID.randomUUID(), providerEventId);
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED, providerEventId);
        RacyAttemptLog attemptLog = new RacyAttemptLog(winner);
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher, meterRegistry);

        assertThatCode(() -> useCase.execute(command())).doesNotThrowAnyException();

        assertThat(attemptLog.tryRecordCalls.get()).isEqualTo(1);
        assertThat(publisher.published).hasSize(1);
        assertThat(publisher.published.get(0).getStatusEventId()).isEqualTo(winner.getStatusEventId());
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

    /** A stored APPROVED attempt standing in for "the row the race's winner already inserted". */
    private static PaymentAttempt approvedAttempt(UUID inboundEventId, UUID providerEventId) {
        return PaymentAttempt.from(
                PAYMENT_ID,
                MERCHANT_ID,
                AMOUNT,
                new ProviderResult(providerEventId, ProviderOutcome.APPROVED, 0L),
                inboundEventId,
                UuidV7.generate(),
                "trace-1",
                "corr-1");
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
            return findByInboundEventId(inboundEventId).isPresent();
        }

        @Override
        public boolean existsByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId) {
            return findByPaymentIdAndProviderEventId(paymentId, providerEventId).isPresent();
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
        public Optional<PaymentAttempt> findLatestByPaymentId(UUID paymentId) {
            return recorded.stream()
                    .filter(a -> a.getPaymentId().equals(paymentId))
                    .max(java.util.Comparator.comparing(PaymentAttempt::getProcessedAt));
        }

        @Override
        public Optional<PaymentAttempt> findByInboundEventId(UUID inboundEventId) {
            return recorded.stream()
                    .filter(a -> a.getCausationEventId().equals(inboundEventId))
                    .findFirst();
        }

        @Override
        public Optional<PaymentAttempt> findByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId) {
            return recorded.stream()
                    .filter(
                            a ->
                                    a.getPaymentId().equals(paymentId)
                                            && a.getProviderEventId().equals(providerEventId))
                    .findFirst();
        }
    }

    /**
     * Simulates the level-1 check-then-act race explicitly: {@code findByInboundEventId} reports
     * "not seen" on its first call (the pre-check, before {@code authorize()}) but returns the
     * winner's row on every call after that - specifically the follow-up re-read the use case
     * makes once {@code tryRecord} (always {@code false} here, standing in for the DB unique
     * constraint) reports it lost the race - as if a concurrent redelivery of the same inbound
     * event won the insert in between.
     */
    private static final class InboundEventRaceAttemptLog implements AttemptLogRepository {
        private final PaymentAttempt winner;
        private final AtomicInteger findByInboundEventIdCalls = new AtomicInteger();
        private final AtomicInteger tryRecordCalls = new AtomicInteger();

        private InboundEventRaceAttemptLog(PaymentAttempt winner) {
            this.winner = winner;
        }

        @Override
        public boolean existsByInboundEventId(UUID inboundEventId) {
            return findByInboundEventIdCalls.get() > 0;
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
        public Optional<PaymentAttempt> findLatestByPaymentId(UUID paymentId) {
            return Optional.empty();
        }

        @Override
        public Optional<PaymentAttempt> findByInboundEventId(UUID inboundEventId) {
            return findByInboundEventIdCalls.incrementAndGet() > 1 ? Optional.of(winner) : Optional.empty();
        }

        @Override
        public Optional<PaymentAttempt> findByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId) {
            return Optional.empty();
        }
    }

    /**
     * Simulates the level-2 check-then-act race explicitly: {@code
     * findByPaymentIdAndProviderEventId} reports "not seen" on the pre-check (as if a concurrent
     * insert of the same key hasn't landed yet when the check runs), but {@code tryRecord} -
     * standing in for the DB unique constraint, the real authority - always reports it lost the
     * race, exactly like {@code PostgresAttemptLogRepository} catching {@code
     * DataIntegrityViolationException} and returning {@code false} instead of letting it
     * propagate. {@code findByInboundEventId} always reports "not seen", so the follow-up re-read
     * correctly attributes this race to level 2, not level 1, and the winner is served from the
     * (paymentId, providerEventId) lookup.
     */
    private static final class RacyAttemptLog implements AttemptLogRepository {
        private final PaymentAttempt winner;
        private final AtomicInteger findByProviderEventIdCalls = new AtomicInteger();
        private final AtomicInteger tryRecordCalls = new AtomicInteger();

        private RacyAttemptLog(PaymentAttempt winner) {
            this.winner = winner;
        }

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
        public Optional<PaymentAttempt> findLatestByPaymentId(UUID paymentId) {
            return Optional.empty();
        }

        @Override
        public Optional<PaymentAttempt> findByInboundEventId(UUID inboundEventId) {
            return Optional.empty();
        }

        @Override
        public Optional<PaymentAttempt> findByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId) {
            return findByProviderEventIdCalls.incrementAndGet() > 1 ? Optional.of(winner) : Optional.empty();
        }
    }

    /**
     * Records every attempt handed to it, in order. {@code failFirstN > 0} makes the first N
     * publish calls throw AFTER recording - standing in for the blocking send failing (broker
     * nack, or the pod dying before the ack), which since the M19 drill 9 fix must propagate to
     * the listener instead of being logged and swallowed.
     */
    private static final class RecordingPublisher implements PaymentStatusPublisher {
        private final List<PaymentAttempt> published = new ArrayList<>();
        private final AtomicInteger pendingCount = new AtomicInteger();
        private final int failFirstN;

        private RecordingPublisher() {
            this(0);
        }

        private RecordingPublisher(int failFirstN) {
            this.failFirstN = failFirstN;
        }

        @Override
        public void publishPending(
                UUID paymentId,
                String merchantId,
                Money amount,
                UUID causationEventId,
                String traceId,
                String correlationId) {
            pendingCount.incrementAndGet();
        }

        @Override
        public void publishStatusChanged(PaymentAttempt attempt) {
            published.add(attempt);
            if (published.size() <= failFirstN) {
                throw new RuntimeException("simulated broker-unacknowledged publish");
            }
        }

        Set<UUID> distinctStatusEventIds() {
            Set<UUID> ids = new HashSet<>();
            published.forEach(a -> ids.add(a.getStatusEventId()));
            return ids;
        }
    }
}
