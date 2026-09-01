package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.ClaimCheckPolicy;
import com.example.psp.paymentapi.domain.model.DocumentReference;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.DisputeDocumentStore;
import com.example.psp.paymentapi.domain.port.DisputeEventPublisher;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class OpenDisputeUseCase {

    private final PaymentRepository paymentRepository;
    private final DisputeDocumentStore documentStore;
    private final DisputeEventPublisher eventPublisher;
    private final long claimCheckThresholdBytes;
    private final boolean claimCheckEnabled;

    public OpenDisputeUseCase(
            PaymentRepository paymentRepository,
            DisputeDocumentStore documentStore,
            DisputeEventPublisher eventPublisher,
            @Value("${payment-api.disputes.claim-check-threshold-bytes}") long claimCheckThresholdBytes,
            @Value("${payment-api.disputes.claim-check-enabled}") boolean claimCheckEnabled) {
        this.paymentRepository = paymentRepository;
        this.documentStore = documentStore;
        this.eventPublisher = eventPublisher;
        this.claimCheckThresholdBytes = claimCheckThresholdBytes;
        this.claimCheckEnabled = claimCheckEnabled;
    }

    public DisputeOutcome execute(OpenDisputeCommand command) {
        Payment payment =
                paymentRepository
                        .findById(command.paymentId())
                        .orElseThrow(
                                () -> new NoSuchElementException("No payment with id=" + command.paymentId()));

        UUID disputeId = UUID.randomUUID();
        long sizeBytes = command.documentBytes().length;

        boolean claimCheck =
                claimCheckEnabled && ClaimCheckPolicy.requiresClaimCheck(sizeBytes, claimCheckThresholdBytes);

        if (claimCheck) {
            DocumentReference reference =
                    documentStore.store(disputeId.toString(), command.documentBytes(), command.contentType());
            eventPublisher.publishClaimChecked(
                    disputeId, payment.getId(), payment.getMerchantId(), command.reason(), reference);
            return new DisputeOutcome(
                    disputeId,
                    payment.getId(),
                    payment.getMerchantId(),
                    sizeBytes,
                    true,
                    reference.bucket(),
                    reference.objectKey());
        }

        eventPublisher.publishInline(
                disputeId,
                payment.getId(),
                payment.getMerchantId(),
                command.reason(),
                command.documentBytes(),
                command.contentType());
        return new DisputeOutcome(
                disputeId, payment.getId(), payment.getMerchantId(), sizeBytes, false, null, null);
    }
}
