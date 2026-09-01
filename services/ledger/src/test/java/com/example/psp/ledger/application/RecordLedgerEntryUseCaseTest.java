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
        assertThat(repository.balanceOf(MERCHANT_ID)).isEqualByComparingTo("100.00");

        // The redelivery: deduplicated before the throw, so no second abort and no double count.
        assertThatCode(() -> useCase.execute(command)).doesNotThrowAnyException();
        assertThat(publisher.published).hasSize(1);
        assertThat(repository.balanceOf(MERCHANT_ID)).isEqualByComparingTo("100.00");
    }

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

    private static final class FakeLedgerRepository implements LedgerRepository {

        private final List<LedgerEntry> appliedEntries = new ArrayList<>();
        private final Set<UUID> constraint = new HashSet<>();
        private final Map<String, BigDecimal> balances = new HashMap<>();
        private final Set<UUID> checkFirstBlindSpots = new HashSet<>();

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
