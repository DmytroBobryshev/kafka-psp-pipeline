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

class ProcessPaymentRequestUseCaseTest {

    private static final UUID PAYMENT_ID = UUID.randomUUID();
    private static final String MERCHANT_ID = "merchant-1";
    private static final Money AMOUNT = new Money(BigDecimal.TEN, "EUR");

    @Test
    void happyPathEmitsPendingThenIpnReceivedThenVerifiedThenTerminalInOrder() {
        FakeProvider provider = new FakeProvider(ProviderOutcome.APPROVED);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ProcessPaymentRequestUseCase useCase = useCase(provider, attemptLog, publisher);

        useCase.execute(command());

        assertThat(publisher.emissionOrder)
                .containsExactly("PENDING", "IPN_RECEIVED", "VERIFIED", "TERMINAL");
    }

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

        assertThatCode(() -> useCase.execute(command)).doesNotThrowAnyException();
        assertThat(publisher.published).isEmpty();
    }

    @Test
    void replayingSameInboundEventIdAuthorizesExactlyOnceAndRepublishesSameEventId() {
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
        assertThat(publisher.emissionOrder)
                .containsExactly("PENDING", "IPN_RECEIVED", "VERIFIED", "TERMINAL", "TERMINAL");
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

    private static ProcessPaymentRequestCommand command(UUID inboundEventId) {
        return new ProcessPaymentRequestCommand(
                PAYMENT_ID, MERCHANT_ID, AMOUNT, inboundEventId, "trace-1", "corr-1");
    }

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

    private static final class RecordingPublisher implements PaymentStatusPublisher {
        private final List<PaymentAttempt> published = new ArrayList<>();
        private final List<String> emissionOrder = new ArrayList<>();
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
            emissionOrder.add("PENDING");
        }

        @Override
        public void publishIpnReceived(
                UUID paymentId,
                String merchantId,
                Money amount,
                UUID providerReference,
                UUID causationEventId,
                String traceId,
                String correlationId) {
            emissionOrder.add("IPN_RECEIVED");
        }

        @Override
        public void publishVerified(
                UUID paymentId,
                String merchantId,
                Money amount,
                UUID providerReference,
                UUID causationEventId,
                String traceId,
                String correlationId) {
            emissionOrder.add("VERIFIED");
        }

        @Override
        public void publishStatusChanged(PaymentAttempt attempt) {
            published.add(attempt);
            emissionOrder.add("TERMINAL");
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
