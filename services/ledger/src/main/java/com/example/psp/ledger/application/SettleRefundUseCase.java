package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.RefundTransitionResult;
import com.example.psp.ledger.domain.model.SettleOutcome;
import com.example.psp.ledger.domain.port.LedgerEntryPublisher;
import com.example.psp.ledger.domain.port.RefundRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * M11 step 4 (happy path): consumes {@code refunds.refund-completed.v1}. Converts an active
 * reservation into a permanent DEBIT {@code ledger_entries} row - the guarded
 * {@code RESERVED -> COMPLETED} transition ({@link RefundRepository#trySettle}) - and publishes
 * {@code ledger.ledger-entry-recorded.v1} via the SAME {@link LedgerEntryPublisher} M7 already
 * uses (this listener runs inside the transactional-producer/read_committed machinery M7 built;
 * see {@code adapters.in.kafka.RefundCompletedListener}).
 *
 * <p>No balance delta is applied here - the amount was already subtracted from the balance at
 * reservation time ({@code ReserveRefundUseCase}); settlement only makes that movement permanent
 * and auditable (see {@link LedgerEntry#debit}).
 *
 * <p>The documented late-completion edge case lives entirely inside
 * {@link RefundRepository#trySettle}: when the saga is no longer RESERVED (typically RELEASED, by
 * compensation or the TTL sweeper), the repository escalates to
 * {@code RefundSagaStatus#NEEDS_MANUAL_REVIEW} instead of applying the debit - this use case only
 * needs to log that loudly, since the escalation itself is already durable by the time this
 * method sees {@link RefundTransitionResult#ESCALATED_MANUAL_REVIEW}.
 */
@Service
public class SettleRefundUseCase {

    private static final Logger log = LoggerFactory.getLogger(SettleRefundUseCase.class);

    private final RefundRepository refundRepository;
    private final LedgerEntryPublisher ledgerEntryPublisher;
    private final Counter settledCounter;
    private final Counter escalatedCounter;
    private final Counter deduplicatedCounter;

    public SettleRefundUseCase(
            RefundRepository refundRepository,
            LedgerEntryPublisher ledgerEntryPublisher,
            MeterRegistry meterRegistry) {
        this.refundRepository = refundRepository;
        this.ledgerEntryPublisher = ledgerEntryPublisher;
        this.settledCounter =
                Counter.builder("ledger.refunds.settled")
                        .description("Refunds converted from RESERVED into a permanent DEBIT entry")
                        .register(meterRegistry);
        this.escalatedCounter =
                Counter.builder("ledger.refunds.escalated-manual-review")
                        .description(
                                "refunds.refund-completed.v1 arrived after the reservation was already "
                                        + "released - the documented late-completion edge case "
                                        + "(sequence-refund-saga.md)")
                        .register(meterRegistry);
        this.deduplicatedCounter =
                Counter.builder("ledger.refunds.deduplicated")
                        .tag("step", "settle")
                        .description("refunds.refund-completed.v1 deliveries already processed")
                        .register(meterRegistry);
    }

    public void execute(SettleRefundCommand command) {
        LedgerEntry debitEntry =
                LedgerEntry.debit(
                        command.inboundEventId(),
                        command.merchantId(),
                        command.paymentId(),
                        command.amount(),
                        command.traceId(),
                        command.correlationId());

        SettleOutcome outcome =
                refundRepository.trySettle(command.refundId(), debitEntry, command.inboundEventId());

        switch (outcome.result()) {
            case APPLIED -> {
                settledCounter.increment();
                log.info(
                        "Settled refundId={} paymentId={} merchantId={} amount={} providerReference={} "
                                + "-> ledger_entries id={} balance={} {} (unchanged by settlement)",
                        command.refundId(),
                        command.paymentId(),
                        command.merchantId(),
                        command.amount().amount(),
                        command.providerReference(),
                        debitEntry.getId(),
                        outcome.balanceAfter().balance().amount(),
                        outcome.balanceAfter().balance().currency());
                ledgerEntryPublisher.publishEntryRecorded(debitEntry, outcome.balanceAfter());
            }
            case ALREADY_APPLIED -> {
                deduplicatedCounter.increment();
                log.info(
                        "Deduplicated refund-completed inboundEventId={} refundId={} - already settled",
                        command.inboundEventId(),
                        command.refundId());
            }
            case ESCALATED_MANUAL_REVIEW -> {
                escalatedCounter.increment();
                log.error(
                        "MANUAL REVIEW REQUIRED: refunds.refund-completed.v1 (inboundEventId={}) arrived "
                                + "for refundId={} paymentId={} merchantId={} amount={} providerReference={} "
                                + "whose reservation was already released - the acquirer moved money after "
                                + "this ledger un-reserved it. NOT applied as a debit (would double-count "
                                + "against the restored balance). See docs/diagrams/sequence-refund-saga.md's "
                                + "documented edge case.",
                        command.inboundEventId(),
                        command.refundId(),
                        command.paymentId(),
                        command.merchantId(),
                        command.amount().amount(),
                        command.providerReference());
            }
            case NOT_APPLICABLE, REJECTED_ILLEGAL -> {
                deduplicatedCounter.increment();
                log.warn(
                        "Rejected illegal transition: refunds.refund-completed.v1 inboundEventId={} "
                                + "refundId={} result={} - see RefundRepository#trySettle for the guard "
                                + "that fired",
                        command.inboundEventId(),
                        command.refundId(),
                        outcome.result());
            }
        }
    }
}
