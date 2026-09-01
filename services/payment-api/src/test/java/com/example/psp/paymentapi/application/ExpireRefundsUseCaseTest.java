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

/**
 * M24: pure unit test of {@link ExpireRefundsUseCase} against fakes - no Spring, no Kafka, no
 * database, same style as {@code ExpirePaymentsUseCaseTest} (which this test mirrors property for
 * property). The properties under test:
 *
 * <ul>
 *   <li>nothing is published when {@link RefundRepository#findExpirationCandidates} returns no
 *       candidates - a genuine no-op tick, not an empty-batch publish. This is the SAME observable
 *       outcome whether the repository found nothing because no refund has crossed its window yet,
 *       or because every refund that HAS crossed it already carries a terminal
 *       {@code refund_status_history} row (COMPLETED/FAILED/EXPIRED) - the {@code NOT EXISTS}
 *       guard in {@code adapters.out.persistence.RefundJpaRepository#findExpirationCandidates}'s
 *       native query is what tells those two cases apart, and it does so entirely inside the SQL;
 *       from this use case's own point of view both simply mean "the candidate list is empty", so
 *       one fake-driven test covers both;
 *   <li>the eventId handed to {@link RefundExpirationEventPublisher#publishExpired} is
 *       deterministic - derived from {@code refundId} alone, so the SAME candidate published on two
 *       separate ticks (a re-sweep because the listener has not yet caught up, or a retried tick
 *       after a transient failure) gets the byte-identical id both times;
 *   <li>the injected {@link Clock} - not {@code Instant.now()} - is what the use case queries the
 *       repository with and stamps on the published {@code occurredAt}.
 * </ul>
 */
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
        // The use case still queried with the clock-derived instant - "nothing found" (whether
        // because nothing is stale yet, or because every stale refund already has a terminal
        // history row) is a real answer from the repository, not a short-circuit that skips the
        // query entirely.
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
        // Simulates the real scenario the deterministic scheme exists for: the candidate still
        // has no terminal refund_status_history row on the NEXT tick (this service's own listener
        // has not yet caught up), so the repository hands it back again - the id must not change
        // between the two ticks.
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

    /** Fake port: hands back a fixed candidate list, recording every instant it was queried with. */
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

    /** Fake port: records every publishExpired call verbatim - no Kafka, no Avro. */
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
