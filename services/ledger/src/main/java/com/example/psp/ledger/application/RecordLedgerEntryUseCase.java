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

/**
 * The single use case: apply one {@code payments.payment-status-changed.v1} event to a merchant
 * balance and publish the resulting {@code ledger.ledger-entry-recorded.v1} entry.
 *
 * <p>{@code application/} orchestrates ports and MAY use Spring annotations, but never imports an
 * adapter type (ADR-0007). Micrometer/SLF4J are cross-cutting observability concerns, not adapter
 * types.
 *
 * <h2>M7: two mechanisms, and they are NOT the same mechanism</h2>
 *
 * <p>This class is where the module's whole point either lands or is lost. There are two separate
 * correctness mechanisms in play across one invocation of {@link #execute}, and conflating them is
 * the classic exactly-once misunderstanding.
 *
 * <h3>Mechanism 1 - Kafka EOS: consume &rarr; process &rarr; produce</h3>
 *
 * <p>Entirely outside this class, and deliberately so. The listener container
 * ({@code config.KafkaConsumerConfig}) runs {@code adapters.in.kafka.PaymentStatusChangedListener}
 * inside a transaction driven by {@code KafkaTransactionManager}, so that:
 *
 * <ol>
 *   <li>the {@link LedgerEntryPublisher} send below joins that open transaction, and
 *   <li>the consumed offsets are added to the <em>same</em> transaction with
 *       {@code sendOffsetsToTransaction(offsets, consumerGroupMetadata)} just before commit.
 * </ol>
 *
 * <p>Both of those things live in Kafka ({@code __consumer_offsets} is itself a Kafka topic), which
 * is exactly why one Kafka transaction can cover them. Returning normally from this method commits
 * that transaction; throwing aborts it.
 *
 * <h3>Mechanism 2 - Postgres idempotency: the M5 pattern, unchanged</h3>
 *
 * <p>{@link LedgerRepository} is a different system with a different transaction log. The Kafka
 * transaction does not reach it and there is no distributed transaction here (nor should there
 * be). So the balance write is made <b>idempotent</b> instead of transactional-with-Kafka, keyed on
 * the inbound envelope {@code eventId} and checked <em>before</em> the balance is mutated - the
 * same two-path shape as {@code psp-connector}'s M5 level 1:
 *
 * <ul>
 *   <li><b>check-first</b> - {@link LedgerRepository#existsByInboundEventId} short-circuits the
 *       common replay case with no write at all;
 *   <li><b>constraint race</b> - {@link LedgerRepository#tryApply} returning empty means a
 *       concurrent delivery of the same inbound event inserted first. The unique constraint is the
 *       authority; losing that race is a normal outcome, never an exception.
 * </ul>
 *
 * <p><b>Replaying the topic cannot double a balance, and that property comes from mechanism 2
 * alone.</b> Delete the transactional producer and balances stay correct; delete the unique
 * constraint and exactly-once Kafka delivery will not save them, because Kafka's guarantee ends at
 * Kafka's boundary. README.md's "Where Kafka EOS ends" says this at length.
 *
 * <h3>Ordering: Postgres first, produce second</h3>
 *
 * <p>The balance write commits <em>before</em> the record is produced. That ordering makes the
 * database the authority on "was this event applied", which is what the dedup check reads. The
 * accepted cost is the one window this design cannot close without an outbox: if the Kafka
 * transaction aborts after the Postgres commit, the redelivered record is deduplicated here and
 * the entry event is never republished. That is a real, documented gap (README "Known issues"),
 * whose fix is M6's transactional-outbox pattern applied to this service - not a bigger Kafka
 * transaction, because no Kafka transaction can be made bigger than Kafka.
 */
@Service
public class RecordLedgerEntryUseCase {

    private static final Logger log = LoggerFactory.getLogger(RecordLedgerEntryUseCase.class);

    /** The only inbound status that moves money. Anything else is recorded nowhere. */
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
            // The abort hook for the read_committed-vs-read_uncommitted experiment. Injected as a
            // plain @Value rather than through a @ConfigurationProperties type so application/
            // acquires no dependency on config/ for a single boolean. Documented in README under
            // "Abort visibility proof"; default false in application.yml, overridden per run with
            //   --ledger.fail-after-produce=true
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
        // A decline is the answer, not an error (ADR-0006 category B) - and it moves no money, so
        // there is nothing to record and nothing to deduplicate: re-applying "no change" any number
        // of times is already idempotent. Returning normally commits the Kafka transaction, which
        // commits the offset, which is what "committed and never retried" means here.
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

        // MECHANISM 2, check-first path. Runs BEFORE the balance is touched, for the same reason
        // psp-connector's M5 level 1 runs before the provider call: the point is to prevent the
        // side effect, not to tidy up after it.
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

        // One Postgres transaction: insert the entry row AND add its signed amount to the balance.
        // This commits independently of - and before - the Kafka transaction that surrounds this
        // whole method.
        Optional<MerchantBalance> balanceAfter = ledgerRepository.tryApply(entry);
        if (balanceAfter.isEmpty()) {
            // MECHANISM 2, constraint-race path: another delivery of this same inbound event
            // inserted between our check above and this write. The database, not our read, is the
            // authority.
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

        // MECHANISM 1: this send joins the Kafka transaction the listener container opened. The
        // record is written to the partition immediately (it has an offset before the transaction
        // commits - that is why an aborted one is visible to a read_uncommitted consumer), but no
        // read_committed consumer will ever see it until the COMMIT marker lands.
        entryPublisher.publishEntryRecorded(entry, balance);

        if (failAfterProduce) {
            // The abort hook: produced, not yet committed. Throwing here makes the container abort
            // the transaction instead of committing it, so the entry above becomes an ABORTED
            // record in ledger.ledger-entry-recorded.v1 and the offsets - which are in the same
            // transaction - are not committed either. See DeliberateAbortException's javadoc for
            // what each isolation level then does with that record, and note that the redelivery
            // is caught by mechanism 2 above, so this throw fires at most once per inbound event.
            throw new DeliberateAbortException(entry.getId(), inboundEventId);
        }

        // Returning normally is the commit: the container calls sendOffsetsToTransaction(...) with
        // the consumed offsets and then commitTransaction(). Offsets and output move together, or
        // neither moves.
    }
}
