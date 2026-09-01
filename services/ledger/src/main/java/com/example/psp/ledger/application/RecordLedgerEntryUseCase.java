package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.exception.DeliberateAbortException;
import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import com.example.psp.ledger.domain.port.LedgerEntryPublisher;
import com.example.psp.ledger.domain.port.LedgerRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class RecordLedgerEntryUseCase {

    private static final Logger log = LoggerFactory.getLogger(RecordLedgerEntryUseCase.class);

    private static final String SUCCEEDED = "SUCCEEDED";

    private final LedgerRepository ledgerRepository;
    private final LedgerEntryPublisher entryPublisher;
    private final boolean failAfterProduce;
    private final Counter appliedCounter;
    private final Counter deduplicatedCheckFirstCounter;
    private final Counter deduplicatedRaceCounter;
    private final Counter ignoredCounter;

    public RecordLedgerEntryUseCase(
            LedgerRepository ledgerRepository,
            LedgerEntryPublisher entryPublisher,
            MeterRegistry meterRegistry,
            @Value("${ledger.fail-after-produce:false}") boolean failAfterProduce) {
        this.ledgerRepository = ledgerRepository;
        this.entryPublisher = entryPublisher;
        this.failAfterProduce = failAfterProduce;
        this.appliedCounter =
                Counter.builder("ledger.entries.applied")
                        .description(
                                "Ledger entries newly applied to a merchant balance and published "
                                        + "(not a duplicate)")
                        .register(meterRegistry);
        this.deduplicatedCheckFirstCounter =
                Counter.builder("ledger.entries.deduplicated")
                        .tag("path", "check-first")
                        .description(
                                "Inbound events skipped because uq_ledger_entries_inbound_event_id "
                                        + "already had the row - the common replay path, caught before "
                                        + "any write. THIS is what keeps balances correct under replay, "
                                        + "not the Kafka transaction.")
                        .register(meterRegistry);
        this.deduplicatedRaceCounter =
                Counter.builder("ledger.entries.deduplicated")
                        .tag("path", "constraint-race")
                        .description(
                                "Inbound events skipped because the unique constraint rejected the "
                                        + "insert - a concurrent delivery of the same event won the "
                                        + "check-then-act race. Normal, never an error.")
                        .register(meterRegistry);
        this.ignoredCounter =
                Counter.builder("ledger.entries.ignored")
                        .description(
                                "Inbound status-changed events that move no money (e.g. DECLINED) - "
                                        + "ADR-0006 category B: a business outcome, committed and never "
                                        + "retried")
                        .register(meterRegistry);
    }

    public void execute(RecordLedgerEntryCommand command) {
        if (!SUCCEEDED.equals(command.status())) {
            ignoredCounter.increment();
            log.info(
                    "Ignoring status={} paymentId={} merchantId={} inboundEventId={} - moves no money",
                    command.status(),
                    command.paymentId(),
                    command.merchantId(),
                    command.inboundEventId());
            return;
        }

        UUID inboundEventId = command.inboundEventId();

        if (ledgerRepository.existsByInboundEventId(inboundEventId)) {
            deduplicatedCheckFirstCounter.increment();
            log.info(
                    "Deduplicated inboundEventId={} paymentId={} merchantId={} path=check-first - "
                            + "balance already reflects this event, skipping write and publish",
                    inboundEventId,
                    command.paymentId(),
                    command.merchantId());
            return;
        }

        LedgerEntry entry =
                LedgerEntry.credit(
                        inboundEventId,
                        command.merchantId(),
                        command.paymentId(),
                        command.amount(),
                        command.traceId(),
                        command.correlationId());

        Optional<MerchantBalance> balanceAfter = ledgerRepository.tryApply(entry);
        if (balanceAfter.isEmpty()) {
            deduplicatedRaceCounter.increment();
            log.info(
                    "Deduplicated inboundEventId={} paymentId={} merchantId={} path=constraint-race "
                            + "- uq_ledger_entries_inbound_event_id rejected the insert, skipping publish",
                    inboundEventId,
                    command.paymentId(),
                    command.merchantId());
            return;
        }

        MerchantBalance balance = balanceAfter.get();
        appliedCounter.increment();
        log.info(
                "Applied ledger entry id={} inboundEventId={} merchantId={} {}{} -> balance={} {} "
                        + "(entryCount={})",
                entry.getId(),
                inboundEventId,
                entry.getMerchantId(),
                entry.getDirection(),
                entry.getAmount().amount(),
                balance.balance().amount(),
                balance.balance().currency(),
                balance.entryCount());

        entryPublisher.publishEntryRecorded(entry, balance);

        if (failAfterProduce) {
            throw new DeliberateAbortException(entry.getId(), inboundEventId);
        }

    }
}
