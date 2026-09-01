package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.RefundStatusHistoryEntry;
import com.example.psp.paymentapi.domain.port.RefundStatusHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * M23: the use case behind all four refund-trail listeners
 * ({@code adapters.in.kafka.RefundStatusChangedListener}, {@code RefundCompletedListener},
 * {@code RefundFailedListener}, {@code RefundFundsReservedListener}). Unlike
 * {@link ApplyPaymentOutcomeUseCase}, there is no state machine to drive here at all - the
 * {@code Refund} aggregate's status is always {@code REQUESTED} and never advances
 * ({@code domain.model.Refund}'s javadoc), so every one of these four listeners does exactly one
 * thing: append a row to {@code refund_status_history}, deduplicated on {@code eventId} by the
 * table's own UNIQUE constraint (V12), never by a check-then-act read here - same
 * {@link RefundStatusHistoryRepository#tryRecord} contract as
 * {@code PaymentStatusHistoryRepository#tryRecord}.
 */
@Service
public class RecordRefundHistoryUseCase {

    private static final Logger log = LoggerFactory.getLogger(RecordRefundHistoryUseCase.class);

    private final RefundStatusHistoryRepository historyRepository;

    public RecordRefundHistoryUseCase(RefundStatusHistoryRepository historyRepository) {
        this.historyRepository = historyRepository;
    }

    public void execute(RecordRefundHistoryCommand command) {
        log.info(
                "Recording refund history refundId={} paymentId={} status={} eventId={}",
                command.refundId(),
                command.paymentId(),
                command.status(),
                command.eventId());

        historyRepository.tryRecord(
                RefundStatusHistoryEntry.record(
                        command.refundId(),
                        command.paymentId(),
                        command.status(),
                        command.providerReference(),
                        command.eventId(),
                        command.occurredAt()));
    }
}
