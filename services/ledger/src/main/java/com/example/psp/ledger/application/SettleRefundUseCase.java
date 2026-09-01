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
