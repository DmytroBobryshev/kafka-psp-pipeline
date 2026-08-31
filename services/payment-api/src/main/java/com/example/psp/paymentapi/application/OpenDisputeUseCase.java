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

/**
 * M13's entry point (ADR-0004: commands enter via REST; everything else is events): the single
 * use case behind {@code POST /api/payments/{paymentId}/disputes}. Validates the payment exists,
 * decides claim-check vs. inline ({@link ClaimCheckPolicy}), and publishes exactly one of
 * {@link DisputeEventPublisher#publishInline}/{@link DisputeEventPublisher#publishClaimChecked}.
 *
 * <h2>Where the threshold lives, and the demo killswitch</h2>
 *
 * <p>{@code claimCheckThresholdBytes} is the ONLY place {@code payment-api.disputes.claim-check-
 * threshold-bytes} is read - {@link ClaimCheckPolicy} itself takes the threshold as a parameter
 * rather than reading configuration, so it stays framework-free (ADR-0007) and independently
 * testable.
 *
 * <p>{@code claimCheckEnabled} exists for exactly one reason, spelled out in full in
 * services/payment-api/README.md's "M13: claim check, measured" section: with it forced to
 * {@code false}, EVERY document is inlined regardless of size, which is what lets the measured
 * demo reproduce a genuine {@code RecordTooLargeException} against Kafka's default {@code
 * max.request.size} (1 MiB) using a real oversized document through the real endpoint, instead of
 * a synthetic unit test standing in for it. It is not a feature a caller can reach - only an
 * operator flipping the ConfigMap/env var can, which is the point: the failure this demonstrates
 * is what claim-check exists to make structurally impossible in normal operation.
 */
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
