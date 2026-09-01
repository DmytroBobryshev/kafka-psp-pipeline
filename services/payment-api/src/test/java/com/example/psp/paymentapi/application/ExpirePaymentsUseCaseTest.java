package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.port.PaymentExpirationEventPublisher;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ExpirePaymentsUseCaseTest {

    private static final Instant FIXED_NOW = Instant.parse("2026-01-01T12:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

    @Test
    void noPublishWhenNothingHasExpired() {
        FakeRepository repository = new FakeRepository(List.of());
        RecordingPublisher publisher = new RecordingPublisher();
        ExpirePaymentsUseCase useCase = new ExpirePaymentsUseCase(repository, publisher, FIXED_CLOCK);

        int published = useCase.execute();

        assertThat(published).isZero();
        assertThat(publisher.published).isEmpty();
        assertThat(repository.queriedWith).containsExactly(FIXED_NOW);
    }

    @Test
    void publishesEveryCandidateWithADeterministicEventId() {
        Payment candidate = payment(UUID.randomUUID(), "merchant-1");
        FakeRepository repository = new FakeRepository(List.of(candidate));
        RecordingPublisher publisher = new RecordingPublisher();
        ExpirePaymentsUseCase useCase = new ExpirePaymentsUseCase(repository, publisher, FIXED_CLOCK);

        int published = useCase.execute();

        assertThat(published).isEqualTo(1);
        assertThat(publisher.published).hasSize(1);
        RecordingPublisher.Call call = publisher.published.get(0);
        assertThat(call.payment()).isEqualTo(candidate);
        assertThat(call.occurredAt()).isEqualTo(FIXED_NOW);
        assertThat(call.eventId())
                .isEqualTo(ExpirePaymentsUseCase.deterministicEventId(candidate.getId()));
    }

    @Test
    void republishingTheSameCandidateOnALaterTickReusesTheSameEventId() {
        Payment candidate = payment(UUID.randomUUID(), "merchant-1");
        FakeRepository repository = new FakeRepository(List.of(candidate));
        RecordingPublisher publisher = new RecordingPublisher();
        ExpirePaymentsUseCase useCase = new ExpirePaymentsUseCase(repository, publisher, FIXED_CLOCK);

        useCase.execute();
        useCase.execute();

        assertThat(publisher.published).hasSize(2);
        assertThat(publisher.published.get(0).eventId()).isEqualTo(publisher.published.get(1).eventId());
    }

    @Test
    void differentCandidatesGetDifferentEventIds() {
        Payment first = payment(UUID.randomUUID(), "merchant-1");
        Payment second = payment(UUID.randomUUID(), "merchant-2");
        FakeRepository repository = new FakeRepository(List.of(first, second));
        RecordingPublisher publisher = new RecordingPublisher();
        ExpirePaymentsUseCase useCase = new ExpirePaymentsUseCase(repository, publisher, FIXED_CLOCK);

        useCase.execute();

        assertThat(publisher.published).hasSize(2);
        assertThat(publisher.published.get(0).eventId())
                .isNotEqualTo(publisher.published.get(1).eventId());
    }

    private static Payment payment(UUID id, String merchantId) {
        return Payment.reconstitute(
                id,
                merchantId,
                Money.of(BigDecimal.TEN, "EUR"),
                PaymentStatus.CREATED,
                FIXED_NOW.minusSeconds(3600),
                null);
    }

    private static final class FakeRepository implements PaymentRepository {
        private final List<Payment> candidates;
        private final List<Instant> queriedWith = new ArrayList<>();

        FakeRepository(List<Payment> candidates) {
            this.candidates = candidates;
        }

        @Override
        public List<Payment> findExpirationCandidates(Instant now) {
            queriedWith.add(now);
            return candidates;
        }

        @Override
        public Payment save(Payment payment) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public Optional<Payment> findById(UUID id) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public void updateStatus(UUID paymentId, PaymentStatus status) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public void applyPendingStatus(UUID paymentId) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public void applyExpiredStatus(UUID paymentId) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public PaymentPage search(String merchantId, PaymentStatus status, int page, int size) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }
    }

    private static final class RecordingPublisher implements PaymentExpirationEventPublisher {
        private final List<Call> published = new ArrayList<>();

        @Override
        public void publishExpired(Payment payment, UUID eventId, Instant occurredAt) {
            published.add(new Call(payment, eventId, occurredAt));
        }

        private record Call(Payment payment, UUID eventId, Instant occurredAt) {
        }
    }
}
