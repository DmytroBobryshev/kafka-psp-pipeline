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

/**
 * M22: pure unit test of {@link ExpirePaymentsUseCase} against fakes - no Spring, no Kafka, no
 * database, same style as every other use-case test in this module. The properties under test:
 *
 * <ul>
 *   <li>nothing is published when {@link PaymentRepository#findExpirationCandidates} returns no
 *       candidates - the scheduler tick is a genuine no-op, not an empty-batch publish;
 *   <li>the eventId handed to {@link PaymentExpirationEventPublisher#publishExpired} is
 *       deterministic - derived from {@code paymentId} alone, so the SAME candidate published on
 *       two separate ticks (a re-sweep because the listener has not yet caught up, or a retried
 *       tick after a transient failure) gets the byte-identical id both times;
 *   <li>the injected {@link Clock} - not {@code Instant.now()} - is what the use case queries the
 *       repository with and stamps on the published {@code occurredAt}, which is what makes this
 *       test deterministic in the first place.
 * </ul>
 */
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
        // The use case still queried with the clock-derived instant - "nothing found" is a real
        // answer from the repository, not a short-circuit that skips the query entirely.
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
        // Simulates the real scenario the deterministic scheme exists for: the candidate is still
        // CREATED/PENDING on the NEXT tick (this service's own listener has not yet caught up), so
        // the repository hands it back again - the id must not change between the two ticks.
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

    /** Fake port: hands back a fixed candidate list, recording every instant it was queried with. */
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

    /** Fake port: records every publishExpired call verbatim - no Kafka, no Avro. */
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
