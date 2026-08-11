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

/**
 * M11 step 2: consumes {@code refunds.refund-requested.v1}. Reserves funds against the merchant's
 * balance - a reservation row plus a balance effect, in one Postgres transaction
 * ({@code RefundRepository#tryReserveOrFail}) - and publishes {@code refunds.funds-reserved.v1}.
 * If the merchant's balance cannot cover the amount, no reservation is made and
 * {@code refunds.refund-failed.v1} is published instead (ADR-0006 category B: a business outcome,
 * not an error - never retried, never DLQ'd).
 *
 * <p>Idempotent the M5/M7 way: {@link RefundRepository#hasProcessedInboundEvent} is the
 * check-first path (the common replay case, caught before any write); the constraint-race path
 * lives inside {@code tryReserveOrFail} itself and is reported back as
 * {@link ReserveOutcome.Decision#ALREADY_PROCESSED}, never by throwing.
 */
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
                // Constraint-race path: a concurrent delivery of the same inbound event won the
                // insert into refund_processed_events between the check-first read above and this
                // call. Normal outcome of at-least-once delivery under concurrency - never an error.
                deduplicatedCounter.increment();
                log.info(
                        "Deduplicated refund-requested inboundEventId={} refundId={} path=constraint-race",
                        command.inboundEventId(),
                        command.refundId());
            }
        }
    }
}
