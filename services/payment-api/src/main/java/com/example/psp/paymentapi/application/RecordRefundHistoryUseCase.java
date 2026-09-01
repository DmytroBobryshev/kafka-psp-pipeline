package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.RefundStatusHistoryEntry;
import com.example.psp.paymentapi.domain.port.RefundStatusHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
