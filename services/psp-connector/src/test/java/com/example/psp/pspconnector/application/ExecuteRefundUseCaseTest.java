package com.example.psp.pspconnector.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.example.psp.common.events.UuidV7;
import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.RefundAttempt;
import com.example.psp.pspconnector.domain.model.RefundOutcome;
import com.example.psp.pspconnector.domain.model.RefundProviderResult;
import com.example.psp.pspconnector.domain.port.RefundAttemptLogRepository;
import com.example.psp.pspconnector.domain.port.RefundProviderPort;
import com.example.psp.pspconnector.domain.port.RefundStatusPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ExecuteRefundUseCaseTest {

    private static final UUID REFUND_ID = UUID.randomUUID();
    private static final UUID PAYMENT_ID = UUID.randomUUID();
    private static final String MERCHANT_ID = "merchant-1";
    private static final Money AMOUNT = new Money(BigDecimal.TEN, "EUR");

    @Test
    void happyPathEmitsPendingThenIpnReceivedThenVerifiedThenTerminalInOrder() {
        FakeProvider provider = new FakeProvider(RefundOutcome.COMPLETED);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ExecuteRefundUseCase useCase = useCase(provider, attemptLog, publisher);

        useCase.execute(command());

        assertThat(publisher.emissionOrder)
                .containsExactly("PENDING", "IPN_RECEIVED", "VERIFIED", "TERMINAL");
    }

    @Test
    void declinedRefundStillEmitsTheFullTrailAsABusinessOutcome() {
        FakeProvider provider = new FakeProvider(RefundOutcome.DECLINED);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        ExecuteRefundUseCase useCase = useCase(provider, attemptLog, publisher);

        assertThatCode(() -> useCase.execute(command())).doesNotThrowAnyException();

        assertThat(publisher.emissionOrder)
                .containsExactly("PENDING", "IPN_RECEIVED", "VERIFIED", "TERMINAL");
    }

    @Test
    void replayingSameInboundEventIdExecutesExactlyOnceAndEmitsOnlyTheTerminalEventOnReplay() {
        UUID inboundEventId = UUID.randomUUID();
        FakeProvider provider = new FakeProvider(RefundOutcome.COMPLETED);
        RecordingAttemptLog attemptLog = new RecordingAttemptLog();
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        ExecuteRefundUseCase useCase = useCase(provider, attemptLog, publisher, meterRegistry);
        ExecuteRefundCommand command = command(inboundEventId);

        useCase.execute(command);
        useCase.execute(command); // the replay: same inbound event, redelivered

        assertThat(provider.refundCallCount()).isEqualTo(1);
        assertThat(attemptLog.recorded).hasSize(1);
        // Trail emitted once on the first delivery, only the terminal event re-emitted on replay.
        assertThat(publisher.emissionOrder)
                .containsExactly("PENDING", "IPN_RECEIVED", "VERIFIED", "TERMINAL", "TERMINAL");
        assertThat(publisher.terminalPublished).hasSize(2);
        assertThat(publisher.distinctStatusEventIds()).hasSize(1);
        assertThat(meterRegistry.counter("psp-connector.refund.attempts.processed").count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.counter("psp-connector.refund.attempts.deduplicated").count())
                .isEqualTo(1.0);
    }

    @Test
    void raceOnInboundEventInsertEmitsOnlyTheWinnersTerminalEvent() {
        UUID inboundEventId = UUID.randomUUID();
        RefundAttempt winner = completedAttempt(inboundEventId);
        FakeProvider provider = new FakeProvider(RefundOutcome.COMPLETED);
        RaceAttemptLog attemptLog = new RaceAttemptLog(winner);
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        ExecuteRefundUseCase useCase = useCase(provider, attemptLog, publisher, meterRegistry);

        assertThatCode(() -> useCase.execute(command(inboundEventId))).doesNotThrowAnyException();

        assertThat(attemptLog.tryRecordCalls.get()).isEqualTo(1);
        assertThat(publisher.emissionOrder).containsExactly("PENDING", "IPN_RECEIVED", "TERMINAL");
        assertThat(publisher.terminalPublished).hasSize(1);
        assertThat(publisher.terminalPublished.get(0).getStatusEventId())
                .isEqualTo(winner.getStatusEventId());
        assertThat(meterRegistry.counter("psp-connector.refund.attempts.deduplicated").count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.counter("psp-connector.refund.attempts.processed").count())
                .isZero();
    }

    private static ExecuteRefundUseCase useCase(
            RefundProviderPort provider, RefundAttemptLogRepository attemptLog, RefundStatusPublisher publisher) {
        return useCase(provider, attemptLog, publisher, new SimpleMeterRegistry());
    }

    private static ExecuteRefundUseCase useCase(
            RefundProviderPort provider,
            RefundAttemptLogRepository attemptLog,
            RefundStatusPublisher publisher,
            MeterRegistry meterRegistry) {
        return new ExecuteRefundUseCase(provider, attemptLog, publisher, meterRegistry);
    }

    private static ExecuteRefundCommand command() {
        return command(UUID.randomUUID());
    }

    private static ExecuteRefundCommand command(UUID inboundEventId) {
        return new ExecuteRefundCommand(
                REFUND_ID, PAYMENT_ID, MERCHANT_ID, AMOUNT, inboundEventId, "trace-1", "corr-1");
    }

    private static RefundAttempt completedAttempt(UUID inboundEventId) {
        return RefundAttempt.from(
                REFUND_ID,
                PAYMENT_ID,
                MERCHANT_ID,
                AMOUNT,
                new RefundProviderResult(UUID.randomUUID(), RefundOutcome.COMPLETED, 0L),
                inboundEventId,
                UuidV7.generate(),
                "trace-1",
                "corr-1");
    }

    private static final class FakeProvider implements RefundProviderPort {
        private final RefundOutcome outcome;
        private final AtomicInteger refundCalls = new AtomicInteger();

        private FakeProvider(RefundOutcome outcome) {
            this.outcome = outcome;
        }

        @Override
        public RefundProviderResult refund(UUID refundId, UUID paymentId, String merchantId, Money amount) {
            refundCalls.incrementAndGet();
            return new RefundProviderResult(UUID.randomUUID(), outcome, 0L);
        }

        int refundCallCount() {
            return refundCalls.get();
        }
    }

    private static final class RecordingAttemptLog implements RefundAttemptLogRepository {
        private final List<RefundAttempt> recorded = new ArrayList<>();

        @Override
        public boolean existsByInboundEventId(UUID inboundEventId) {
            return findByInboundEventId(inboundEventId).isPresent();
        }

        @Override
        public boolean tryRecord(RefundAttempt attempt) {
            if (existsByInboundEventId(attempt.getCausationEventId())) {
                return false;
            }
            recorded.add(attempt);
            return true;
        }

        @Override
        public Optional<RefundAttempt> findByInboundEventId(UUID inboundEventId) {
            return recorded.stream().filter(a -> a.getCausationEventId().equals(inboundEventId)).findFirst();
        }
    }

    private static final class RaceAttemptLog implements RefundAttemptLogRepository {
        private final RefundAttempt winner;
        private final AtomicInteger findByInboundEventIdCalls = new AtomicInteger();
        final AtomicInteger tryRecordCalls = new AtomicInteger();

        private RaceAttemptLog(RefundAttempt winner) {
            this.winner = winner;
        }

        @Override
        public boolean existsByInboundEventId(UUID inboundEventId) {
            return false;
        }

        @Override
        public boolean tryRecord(RefundAttempt attempt) {
            tryRecordCalls.incrementAndGet();
            return false;
        }

        @Override
        public Optional<RefundAttempt> findByInboundEventId(UUID inboundEventId) {
            return findByInboundEventIdCalls.incrementAndGet() > 1 ? Optional.of(winner) : Optional.empty();
        }
    }

    private static final class RecordingPublisher implements RefundStatusPublisher {
        private final List<RefundAttempt> terminalPublished = new ArrayList<>();
        private final List<String> emissionOrder = new ArrayList<>();

        @Override
        public void publishPending(
                UUID refundId,
                UUID paymentId,
                String merchantId,
                Money amount,
                UUID causationEventId,
                String traceId,
                String correlationId) {
            emissionOrder.add("PENDING");
        }

        @Override
        public void publishIpnReceived(
                UUID refundId,
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
                UUID refundId,
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
        public void publishOutcome(RefundAttempt attempt) {
            terminalPublished.add(attempt);
            emissionOrder.add("TERMINAL");
        }

        java.util.Set<UUID> distinctStatusEventIds() {
            java.util.Set<UUID> ids = new java.util.HashSet<>();
            terminalPublished.forEach(a -> ids.add(a.getStatusEventId()));
            return ids;
        }
    }
}
