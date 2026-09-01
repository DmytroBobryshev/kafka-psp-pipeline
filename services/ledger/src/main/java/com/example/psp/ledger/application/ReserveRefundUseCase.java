package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.RefundReservation;
import com.example.psp.ledger.domain.model.RefundRequest;
import com.example.psp.ledger.domain.model.ReserveOutcome;
import com.example.psp.ledger.domain.port.RefundEventPublisher;
import com.example.psp.ledger.domain.port.RefundRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ReserveRefundUseCase {

    private static final Logger log = LoggerFactory.getLogger(ReserveRefundUseCase.class);
    private static final String INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE";

    private final RefundRepository refundRepository;
    private final RefundEventPublisher refundEventPublisher;
    private final Counter reservedCounter;
    private final Counter insufficientBalanceCounter;
    private final Counter deduplicatedCounter;

    public ReserveRefundUseCase(
            RefundRepository refundRepository,
            RefundEventPublisher refundEventPublisher,
            MeterRegistry meterRegistry) {
        this.refundRepository = refundRepository;
        this.refundEventPublisher = refundEventPublisher;
        this.reservedCounter =
                Counter.builder("ledger.refunds.reserved")
                        .description("Refund requests reserved against the merchant balance")
                        .register(meterRegistry);
        this.insufficientBalanceCounter =
                Counter.builder("ledger.refunds.insufficient-balance")
                        .description(
                                "Refund requests declined at the reservation step - ADR-0006 category B, "
                                        + "never retried")
                        .register(meterRegistry);
        this.deduplicatedCounter =
                Counter.builder("ledger.refunds.deduplicated")
                        .tag("step", "reserve")
                        .description("refunds.refund-requested.v1 deliveries already processed")
                        .register(meterRegistry);
    }

    public void execute(ReserveRefundCommand command) {
        if (refundRepository.hasProcessedInboundEvent(command.inboundEventId())) {
            deduplicatedCounter.increment();
            log.info(
                    "Deduplicated refund-requested inboundEventId={} refundId={} - already processed, "
                            + "skipping",
                    command.inboundEventId(),
                    command.refundId());
            return;
        }

        RefundReservation reservation =
                RefundReservation.newReservation(
                        command.refundId(), command.paymentId(), command.merchantId(), command.amount());

        ReserveOutcome outcome =
                refundRepository.tryReserveOrFail(
                        reservation, command.inboundEventId(), INSUFFICIENT_BALANCE);

        RefundRequest request =
                new RefundRequest(
                        command.refundId(), command.paymentId(), command.merchantId(), command.amount());

        switch (outcome.decision()) {
            case RESERVED -> {
                reservedCounter.increment();
                log.info(
                        "Reserved refundId={} paymentId={} merchantId={} amount={} -> balance={} {}",
                        command.refundId(),
                        command.paymentId(),
                        command.merchantId(),
                        command.amount().amount(),
                        outcome.balanceAfter().balance().amount(),
                        outcome.balanceAfter().balance().currency());
                refundEventPublisher.publishFundsReserved(
                        request,
                        command.inboundEventId(),
                        command.traceId(),
                        command.correlationId(),
                        outcome.balanceAfter());
            }
            case INSUFFICIENT_BALANCE -> {
                insufficientBalanceCounter.increment();
                log.info(
                        "Refund refundId={} paymentId={} merchantId={} amount={} declined at reservation - "
                                + "insufficient balance (ADR-0006 category B)",
                        command.refundId(),
                        command.paymentId(),
                        command.merchantId(),
                        command.amount().amount());
                refundEventPublisher.publishRefundFailedInsufficientBalance(
                        request, command.inboundEventId(), command.traceId(), command.correlationId());
            }
            case ALREADY_PROCESSED -> {
                deduplicatedCounter.increment();
                log.info(
                        "Deduplicated refund-requested inboundEventId={} refundId={} path=constraint-race",
                        command.inboundEventId(),
                        command.refundId());
            }
        }
    }
}
