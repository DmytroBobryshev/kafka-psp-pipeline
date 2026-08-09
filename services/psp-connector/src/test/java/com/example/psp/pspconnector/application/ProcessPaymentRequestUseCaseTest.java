package com.example.psp.pspconnector.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.pspconnector.domain.exception.ProviderTimeoutException;
import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.model.ProviderResult;
import com.example.psp.pspconnector.domain.port.AttemptLogRepository;
import com.example.psp.pspconnector.domain.port.PaymentProviderPort;
import com.example.psp.pspconnector.domain.port.PaymentStatusPublisher;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Plain JUnit against {@code application/} + {@code domain/} - no Spring context, no Kafka, no
 * database, same pattern as {@code payment-api}'s {@code CreatePaymentUseCaseTest}. Exercises the
 * ADR-0006 branch this module exists to demonstrate: TIMEOUT never publishes and always throws;
 * APPROVED/DECLINED always publish and never throw.
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
        ProcessPaymentRequestUseCase useCase =
                new ProcessPaymentRequestUseCase(provider, attemptLog, publisher);

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
        ProcessPaymentRequestUseCase useCase =
                new ProcessPaymentRequestUseCase(provider, attemptLog, publisher);

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
        ProcessPaymentRequestUseCase useCase =
                new ProcessPaymentRequestUseCase(provider, attemptLog, publisher);

        // ADR-0006 category A: retryable - propagates so the Kafka listener never acks.
        assertThatThrownBy(() -> useCase.execute(command())).isInstanceOf(ProviderTimeoutException.class);

        // Still recorded - "just record attempts", even the ones that time out.
        assertThat(attemptLog.recorded).hasSize(1);
        assertThat(attemptLog.recorded.get(0).getOutcome()).isEqualTo(ProviderOutcome.TIMEOUT);
        assertThat(publisher.publishedCount.get()).isZero();
    }

    private static ProcessPaymentRequestCommand command() {
        return new ProcessPaymentRequestCommand(
                PAYMENT_ID, MERCHANT_ID, AMOUNT, UUID.randomUUID(), "trace-1", "corr-1");
    }

    private static final class FakeProvider implements PaymentProviderPort {
        private final ProviderOutcome outcome;

        private FakeProvider(ProviderOutcome outcome) {
            this.outcome = outcome;
        }

        @Override
        public ProviderResult authorize(UUID paymentId, String merchantId, Money amount) {
            return new ProviderResult(UUID.randomUUID(), outcome, 0L);
        }
    }

    private static final class RecordingAttemptLog implements AttemptLogRepository {
        private final List<PaymentAttempt> recorded = new ArrayList<>();

        @Override
        public void record(PaymentAttempt attempt) {
            recorded.add(attempt);
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
