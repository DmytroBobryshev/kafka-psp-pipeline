package com.example.psp.ledger.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.ledger.domain.exception.DeliberateAbortException;
import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import com.example.psp.ledger.domain.model.Money;
import com.example.psp.ledger.domain.port.LedgerEntryPublisher;
import com.example.psp.ledger.domain.port.LedgerRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Plain JUnit against {@code application/} + {@code domain/} - no Spring context, no Kafka broker,
 * no database, same pattern as {@code psp-connector}'s {@code ProcessPaymentRequestUseCaseTest}.
 *
 * <p>What is being tested here is <b>mechanism 2</b>, deliberately and exclusively: the Postgres
 * idempotency that keeps balances correct. The fake {@link LedgerRepository} below reproduces the
 * real adapter's contract (a unique constraint on {@code inboundEventId} that both a check-first
 * read and an insert consult) and nothing else - there is no transaction of any kind in this test,
 * which is exactly the point being made. <b>Every balance assertion below would hold with the
 * transactional producer deleted</b>; none of them would hold with the unique constraint deleted.
 * If a future change makes these tests depend on a broker, the two mechanisms have been conflated.
 *
 * <p>Mechanism 1 (the Kafka transaction) is not fake-able in a meaningful way - a fake that
 * "aborts" proves nothing about what a real transaction coordinator does with markers and the Last
 * Stable Offset. It is verified for real against the live compose stack instead, and the abort hook
 * used to do that is covered here only at the level of "it throws after publishing, not before"
 * ({@link #failAfterProduceThrowsOnlyAfterPublishingAndOnlyOnce()}), because that ordering is a
 * property of this class.
 */
class RecordLedgerEntryUseCaseTest {

    private static final String MERCHANT_ID = "merchant-1";
    private static final Money AMOUNT = Money.of(new BigDecimal("100.00"), "EUR");

    @Test
    void succeededEventAppliesEntryAndPublishesOnce() {
        FakeLedgerRepository repository = new FakeLedgerRepository();
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        RecordLedgerEntryUseCase useCase = useCase(repository, publisher, meterRegistry, false);

        useCase.execute(command(UUID.randomUUID(), "SUCCEEDED"));

        assertThat(repository.appliedEntries).hasSize(1);
        assertThat(repository.balanceOf(MERCHANT_ID)).isEqualByComparingTo("100.00");
        assertThat(publisher.published).hasSize(1);
        assertThat(publisher.published.get(0).balanceAfter().balance().amount())
                .isEqualByComparingTo("100.00");
        assertThat(meterRegistry.counter("ledger.entries.applied").count()).isEqualTo(1.0);
    }

    @Test
    void replayingSameInboundEventIdDoesNotDoubleTheBalance() {
        // THE M7 assertion. Redelivering the same inbound event - after a rebalance, a crash, an
        // aborted Kafka transaction, or an operator resetting ledger.v1's offsets to earliest and
        // replaying the whole topic - must leave the balance exactly where it was.
        //
        // Note what is NOT in this test: any transaction, transactional producer, isolation level
        // or offset. Kafka exactly-once contributes nothing to this property, because the balance
        // does not live in Kafka.
        UUID inboundEventId = UUID.randomUUID();
        FakeLedgerRepository repository = new FakeLedgerRepository();
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        RecordLedgerEntryUseCase useCase = useCase(repository, publisher, meterRegistry, false);
        RecordLedgerEntryCommand command = command(inboundEventId, "SUCCEEDED");

        useCase.execute(command);
        useCase.execute(command); // the replay
        useCase.execute(command); // and again, for good measure

        assertThat(repository.balanceOf(MERCHANT_ID)).isEqualByComparingTo("100.00");
        assertThat(repository.appliedEntries).hasSize(1);
        assertThat(publisher.published).hasSize(1);
        assertThat(meterRegistry.counter("ledger.entries.applied").count()).isEqualTo(1.0);
        assertThat(meterRegistry.counter("ledger.entries.deduplicated", "path", "check-first").count())
                .isEqualTo(2.0);
        assertThat(
                        meterRegistry
                                .counter("ledger.entries.deduplicated", "path", "constraint-race")
                                .count())
                .isZero();
    }

    @Test
    void distinctInboundEventIdsForSameMerchantAccumulate() {
        // The other half of the property: dedup must key on the INBOUND EVENT, not on the merchant
        // or the amount. A dedup that also swallowed genuinely new events would produce a balance
        // that is stable and wrong, which is worse than one that is unstable and wrong.
        FakeLedgerRepository repository = new FakeLedgerRepository();
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        RecordLedgerEntryUseCase useCase = useCase(repository, publisher, meterRegistry, false);

        useCase.execute(command(UUID.randomUUID(), "SUCCEEDED"));
        useCase.execute(command(UUID.randomUUID(), "SUCCEEDED"));
        useCase.execute(command(UUID.randomUUID(), "SUCCEEDED"));

        assertThat(repository.balanceOf(MERCHANT_ID)).isEqualByComparingTo("300.00");
        assertThat(repository.appliedEntries).hasSize(3);
        assertThat(publisher.published).hasSize(3);
        assertThat(meterRegistry.counter("ledger.entries.applied").count()).isEqualTo(3.0);
        assertThat(meterRegistry.counter("ledger.entries.deduplicated", "path", "check-first").count())
                .isZero();
    }

    @Test
    void constraintRaceIsHandledWithoutThrowingAndWithoutDoubleCounting() {
        // The check-first read is a check-then-act and is racy by construction: two concurrent
        // deliveries of the same inbound event can both pass it. The unique constraint is the real
        // authority, and losing to it is a NORMAL outcome that must never surface as an exception -
        // rethrowing here would abort the Kafka transaction, redeliver the record, and hit the same
        // constraint forever.
        UUID inboundEventId = UUID.randomUUID();
        FakeLedgerRepository repository = new FakeLedgerRepository();
        repository.suppressCheckFirstFor(inboundEventId); // simulate losing the race
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        RecordLedgerEntryUseCase useCase = useCase(repository, publisher, meterRegistry, false);
        RecordLedgerEntryCommand command = command(inboundEventId, "SUCCEEDED");

        useCase.execute(command);
        assertThatCode(() -> useCase.execute(command)).doesNotThrowAnyException();

        assertThat(repository.balanceOf(MERCHANT_ID)).isEqualByComparingTo("100.00");
        assertThat(repository.appliedEntries).hasSize(1);
        assertThat(publisher.published).hasSize(1);
        assertThat(
                        meterRegistry
                                .counter("ledger.entries.deduplicated", "path", "constraint-race")
                                .count())
                .isEqualTo(1.0);
    }

    @Test
    void declinedEventMovesNoMoneyAndPublishesNothing() {
        // ADR-0006 category B: a decline is a business outcome, not an error. Committed, never
        // retried, and it produces no ledger entry because no money moved.
        FakeLedgerRepository repository = new FakeLedgerRepository();
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        RecordLedgerEntryUseCase useCase = useCase(repository, publisher, meterRegistry, false);

        assertThatCode(() -> useCase.execute(command(UUID.randomUUID(), "DECLINED")))
                .doesNotThrowAnyException();

        assertThat(repository.appliedEntries).isEmpty();
        assertThat(publisher.published).isEmpty();
        assertThat(meterRegistry.counter("ledger.entries.ignored").count()).isEqualTo(1.0);
    }

    @Test
    void failAfterProduceThrowsOnlyAfterPublishingAndOnlyOnce() {
        // The abort hook's two required properties, both of which are properties of THIS class
        // rather than of Kafka:
        //   1. the throw happens AFTER the record has been produced - otherwise there is no aborted
        //      record in the log and the whole read_uncommitted experiment has nothing to observe;
        //   2. the redelivery that follows is short-circuited by the dedup check BEFORE reaching
        //      the throw, so the abort fires at most once per inbound event and the consumer makes
        //      progress instead of looping.
        UUID inboundEventId = UUID.randomUUID();
        FakeLedgerRepository repository = new FakeLedgerRepository();
        RecordingPublisher publisher = new RecordingPublisher();
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        RecordLedgerEntryUseCase useCase = useCase(repository, publisher, meterRegistry, true);
        RecordLedgerEntryCommand command = command(inboundEventId, "SUCCEEDED");

        assertThatThrownBy(() -> useCase.execute(command))
                .isInstanceOf(DeliberateAbortException.class);

        // Produced before the throw: property 1.
        assertThat(publisher.published).hasSize(1);
        // And the Postgres side committed regardless, because it is NOT in the Kafka transaction -
        // the entire lesson of the module, visible as a test assertion.
        assertThat(repository.balanceOf(MERCHANT_ID)).isEqualByComparingTo("100.00");

        // The redelivery: deduplicated before the throw, so no second abort and no double count.
        assertThatCode(() -> useCase.execute(command)).doesNotThrowAnyException();
        assertThat(publisher.published).hasSize(1);
        assertThat(repository.balanceOf(MERCHANT_ID)).isEqualByComparingTo("100.00");
    }

    // ---------------------------------------------------------------------------------------------
    // Fakes - hand-written, no mocking framework, same style as psp-connector's use-case test.
    // ---------------------------------------------------------------------------------------------

    private RecordLedgerEntryUseCase useCase(
            LedgerRepository repository,
            LedgerEntryPublisher publisher,
            MeterRegistry meterRegistry,
            boolean failAfterProduce) {
        return new RecordLedgerEntryUseCase(repository, publisher, meterRegistry, failAfterProduce);
    }

    private RecordLedgerEntryCommand command(UUID inboundEventId, String status) {
        return new RecordLedgerEntryCommand(
                inboundEventId,
                UUID.randomUUID(),
                MERCHANT_ID,
                AMOUNT,
                status,
                "trace-1",
                "correlation-1");
    }

    /**
     * Reproduces the real adapter's contract: a unique constraint on {@code inboundEventId} that
     * both {@link #existsByInboundEventId} and {@link #tryApply} consult, and an atomic
     * "insert entry + add delta" that either happens completely or not at all.
     */
    private static final class FakeLedgerRepository implements LedgerRepository {

        private final List<LedgerEntry> appliedEntries = new ArrayList<>();
        private final Set<UUID> constraint = new HashSet<>();
        private final Map<String, BigDecimal> balances = new HashMap<>();
        private final Set<UUID> checkFirstBlindSpots = new HashSet<>();

        /**
         * Makes {@link #existsByInboundEventId} lie exactly once per id, so the use case walks past
         * its check-first path into the insert and loses to the constraint there - which is what a
         * genuine concurrent delivery looks like from inside a single-threaded test.
         */
        void suppressCheckFirstFor(UUID inboundEventId) {
            checkFirstBlindSpots.add(inboundEventId);
        }

        @Override
        public boolean existsByInboundEventId(UUID inboundEventId) {
            if (checkFirstBlindSpots.contains(inboundEventId)) {
                return false;
            }
            return constraint.contains(inboundEventId);
        }

        @Override
        public Optional<MerchantBalance> tryApply(LedgerEntry entry) {
            if (!constraint.add(entry.getInboundEventId())) {
                // The unique constraint rejected it. Nothing is applied, and this is reported by
                // return value - never by throwing (LedgerRepository#tryApply's contract).
                return Optional.empty();
            }
            appliedEntries.add(entry);
            BigDecimal balance =
                    balances
                            .getOrDefault(entry.getMerchantId(), BigDecimal.ZERO)
                            .add(entry.signedAmount());
            balances.put(entry.getMerchantId(), balance);
            return Optional.of(
                    new MerchantBalance(
                            entry.getMerchantId(),
                            Money.of(balance, entry.getAmount().currency()),
                            appliedEntries.stream()
                                    .filter(e -> e.getMerchantId().equals(entry.getMerchantId()))
                                    .count(),
                            Instant.now()));
        }

        BigDecimal balanceOf(String merchantId) {
            return balances.getOrDefault(merchantId, BigDecimal.ZERO);
        }
    }

    private record PublishedEntry(LedgerEntry entry, MerchantBalance balanceAfter) {}

    private static final class RecordingPublisher implements LedgerEntryPublisher {

        private final List<PublishedEntry> published = new ArrayList<>();

        @Override
        public void publishEntryRecorded(LedgerEntry entry, MerchantBalance balanceAfter) {
            published.add(new PublishedEntry(entry, balanceAfter));
        }
    }
}
