package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Refund;
import com.example.psp.paymentapi.domain.port.RefundExpirationEventPublisher;
import com.example.psp.paymentapi.domain.port.RefundRepository;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ExpireRefundsUseCaseTest {

    private static final Instant FIXED_NOW = Instant.parse("2026-01-01T12:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

    @Test
    void noPublishWhenNothingHasExpired() {
        FakeRepository repository = new FakeRepository(List.of());
        RecordingPublisher publisher = new RecordingPublisher();
        ExpireRefundsUseCase useCase = new ExpireRefundsUseCase(repository, publisher, FIXED_CLOCK);

        int published = useCase.execute();

        assertThat(published).isZero();
        assertThat(publisher.published).isEmpty();
        assertThat(repository.queriedWith).containsExactly(FIXED_NOW);
    }

    @Test
    void publishesEveryCandidateWithADeterministicEventId() {
        Refund candidate = refund(UUID.randomUUID(), "merchant-1");
        FakeRepository repository = new FakeRepository(List.of(candidate));
        RecordingPublisher publisher = new RecordingPublisher();
        ExpireRefundsUseCase useCase = new ExpireRefundsUseCase(repository, publisher, FIXED_CLOCK);

        int published = useCase.execute();

        assertThat(published).isEqualTo(1);
        assertThat(publisher.published).hasSize(1);
        RecordingPublisher.Call call = publisher.published.get(0);
        assertThat(call.refund()).isEqualTo(candidate);
        assertThat(call.occurredAt()).isEqualTo(FIXED_NOW);
        assertThat(call.eventId())
                .isEqualTo(ExpireRefundsUseCase.deterministicEventId(candidate.getId()));
    }

    @Test
    void republishingTheSameCandidateOnALaterTickReusesTheSameEventId() {
        Refund candidate = refund(UUID.randomUUID(), "merchant-1");
        FakeRepository repository = new FakeRepository(List.of(candidate));
        RecordingPublisher publisher = new RecordingPublisher();
        ExpireRefundsUseCase useCase = new ExpireRefundsUseCase(repository, publisher, FIXED_CLOCK);

        useCase.execute();
        useCase.execute();

        assertThat(publisher.published).hasSize(2);
        assertThat(publisher.published.get(0).eventId()).isEqualTo(publisher.published.get(1).eventId());
    }

    @Test
    void differentCandidatesGetDifferentEventIds() {
        Refund first = refund(UUID.randomUUID(), "merchant-1");
        Refund second = refund(UUID.randomUUID(), "merchant-2");
        FakeRepository repository = new FakeRepository(List.of(first, second));
        RecordingPublisher publisher = new RecordingPublisher();
        ExpireRefundsUseCase useCase = new ExpireRefundsUseCase(repository, publisher, FIXED_CLOCK);

        useCase.execute();

        assertThat(publisher.published).hasSize(2);
        assertThat(publisher.published.get(0).eventId())
                .isNotEqualTo(publisher.published.get(1).eventId());
    }

    private static Refund refund(UUID id, String merchantId) {
        return Refund.reconstitute(
                id,
                UUID.randomUUID(),
                merchantId,
                Money.of(BigDecimal.TEN, "EUR"),
                "requested by merchant",
                FIXED_NOW.minusSeconds(3600));
    }

    private static final class FakeRepository implements RefundRepository {
        private final List<Refund> candidates;
        private final List<Instant> queriedWith = new ArrayList<>();

        FakeRepository(List<Refund> candidates) {
            this.candidates = candidates;
        }

        @Override
        public List<Refund> findExpirationCandidates(Instant now) {
            queriedWith.add(now);
            return candidates;
        }

        @Override
        public Refund save(Refund refund) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public BigDecimal sumRequestedAmount(UUID paymentId) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public List<Refund> findByPaymentId(UUID paymentId) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public Optional<Refund> findByIdAndPaymentId(UUID id, UUID paymentId) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }
    }

    private static final class RecordingPublisher implements RefundExpirationEventPublisher {
        private final List<Call> published = new ArrayList<>();

        @Override
        public void publishExpired(Refund refund, UUID eventId, Instant occurredAt) {
            published.add(new Call(refund, eventId, occurredAt));
        }

        private record Call(Refund refund, UUID eventId, Instant occurredAt) {
        }
    }
}
