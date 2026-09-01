package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import com.example.psp.paymentapi.domain.port.PaymentStatusHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ApplyPaymentOutcomeUseCase {

    private static final Logger log = LoggerFactory.getLogger(ApplyPaymentOutcomeUseCase.class);

    private final PaymentRepository paymentRepository;
    private final PaymentStatusHistoryRepository historyRepository;

    public ApplyPaymentOutcomeUseCase(
            PaymentRepository paymentRepository, PaymentStatusHistoryRepository historyRepository) {
        this.paymentRepository = paymentRepository;
        this.historyRepository = historyRepository;
    }

    @Transactional
    public void execute(ApplyPaymentOutcomeCommand command) {
        log.info(
                "Applying payment outcome paymentId={} rawStatus={} domainStatus={} eventId={}",
                command.paymentId(),
                command.rawStatus(),
                command.domainStatus(),
                command.eventId());

        if (command.domainStatus() == PaymentStatus.PENDING) {
            paymentRepository.applyPendingStatus(command.paymentId());
        } else if (command.domainStatus() == PaymentStatus.EXPIRED) {
            // M22: conditional, like PENDING above - never downgrades an already-resolved payment.
            paymentRepository.applyExpiredStatus(command.paymentId());
        } else if (command.domainStatus() != null) {
            paymentRepository.updateStatus(command.paymentId(), command.domainStatus());
        }

        historyRepository.tryRecord(
                PaymentStatusHistoryEntry.record(
                        command.paymentId(),
                        command.rawStatus(),
                        command.providerReference(),
                        command.eventId(),
                        command.occurredAt()));
    }
}
