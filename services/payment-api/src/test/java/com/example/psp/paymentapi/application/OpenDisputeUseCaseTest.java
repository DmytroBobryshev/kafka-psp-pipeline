package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.paymentapi.domain.model.DocumentReference;
import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.DisputeDocumentStore;
import com.example.psp.paymentapi.domain.port.DisputeEventPublisher;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OpenDisputeUseCaseTest {

    private static final long THRESHOLD_BYTES = 100L;
    private static final UUID PAYMENT_ID = UUID.randomUUID();
    private static final String MERCHANT_ID = "acme";

    private final FakePaymentRepository payments = new FakePaymentRepository();
    private final RecordingDocumentStore documentStore = new RecordingDocumentStore();
    private final RecordingEventPublisher eventPublisher = new RecordingEventPublisher();

    private OpenDisputeUseCase useCase(boolean claimCheckEnabled) {
        return new OpenDisputeUseCase(
                payments, documentStore, eventPublisher, THRESHOLD_BYTES, claimCheckEnabled);
    }

    @Test
    void aSmallDocumentIsPublishedInlineAndNeverTouchesTheStore() {
        payments.save(Payment.reconstitute(
                PAYMENT_ID, MERCHANT_ID, Money.of(BigDecimal.TEN, "EUR"),
                com.example.psp.paymentapi.domain.model.PaymentStatus.CREATED, java.time.Instant.now(), null));
        byte[] smallDocument = new byte[(int) THRESHOLD_BYTES]; // exactly at threshold -> inline

        DisputeOutcome outcome =
                useCase(true)
                        .execute(new OpenDisputeCommand(PAYMENT_ID, "goods not received", smallDocument, "text/plain"));

        assertThat(outcome.claimChecked()).isFalse();
        assertThat(outcome.bucket()).isNull();
        assertThat(outcome.sizeBytes()).isEqualTo(THRESHOLD_BYTES);
        assertThat(documentStore.stored).isEmpty();
        assertThat(eventPublisher.inlineCalls).hasSize(1);
        assertThat(eventPublisher.claimCheckedCalls).isEmpty();
    }

    @Test
    void aLargeDocumentIsUploadedThenPublishedAsAReference() {
        payments.save(Payment.reconstitute(
                PAYMENT_ID, MERCHANT_ID, Money.of(BigDecimal.TEN, "EUR"),
                com.example.psp.paymentapi.domain.model.PaymentStatus.CREATED, java.time.Instant.now(), null));
        byte[] largeDocument = new byte[(int) THRESHOLD_BYTES + 1];

        DisputeOutcome outcome =
                useCase(true)
                        .execute(new OpenDisputeCommand(PAYMENT_ID, "duplicate charge", largeDocument, "application/pdf"));

        assertThat(outcome.claimChecked()).isTrue();
        assertThat(outcome.bucket()).isEqualTo("disputes");
        assertThat(outcome.objectKey()).isEqualTo(outcome.disputeId().toString());
        assertThat(documentStore.stored).hasSize(1);
        assertThat(eventPublisher.claimCheckedCalls).hasSize(1);
        assertThat(eventPublisher.inlineCalls).isEmpty();
    }

    @Test
    void theDisabledKillswitchForcesInlineEvenAboveThreshold() {
        payments.save(Payment.reconstitute(
                PAYMENT_ID, MERCHANT_ID, Money.of(BigDecimal.TEN, "EUR"),
                com.example.psp.paymentapi.domain.model.PaymentStatus.CREATED, java.time.Instant.now(), null));
        byte[] largeDocument = new byte[(int) THRESHOLD_BYTES * 10];

        DisputeOutcome outcome =
                useCase(false)
                        .execute(new OpenDisputeCommand(PAYMENT_ID, "chargeback", largeDocument, "application/pdf"));

        assertThat(outcome.claimChecked()).isFalse();
        assertThat(documentStore.stored).isEmpty();
        assertThat(eventPublisher.inlineCalls).hasSize(1);
    }

    @Test
    void anUnknownPaymentIsRejectedBeforeAnythingIsPublished() {
        OpenDisputeCommand command =
                new OpenDisputeCommand(UUID.randomUUID(), "reason", new byte[]{1, 2, 3}, "text/plain");

        assertThatThrownBy(() -> useCase(true).execute(command))
                .isInstanceOf(NoSuchElementException.class);

        assertThat(documentStore.stored).isEmpty();
        assertThat(eventPublisher.inlineCalls).isEmpty();
        assertThat(eventPublisher.claimCheckedCalls).isEmpty();
    }

    private static final class FakePaymentRepository implements PaymentRepository {
        private final Map<UUID, Payment> byId = new HashMap<>();

        @Override
        public Payment save(Payment payment) {
            byId.put(payment.getId(), payment);
            return payment;
        }

        @Override
        public Optional<Payment> findById(UUID id) {
            return Optional.ofNullable(byId.get(id));
        }

        @Override
        public void updateStatus(UUID paymentId, com.example.psp.paymentapi.domain.model.PaymentStatus status) {
            throw new UnsupportedOperationException("not needed by this test");
        }

        @Override
        public void applyPendingStatus(UUID paymentId) {
            throw new UnsupportedOperationException("not needed by this test");
        }

        @Override
        public void applyExpiredStatus(UUID paymentId) {
            throw new UnsupportedOperationException("not needed by this test");
        }

        @Override
        public java.util.List<Payment> findExpirationCandidates(java.time.Instant now) {
            throw new UnsupportedOperationException("not needed by this test");
        }

        @Override
        public com.example.psp.paymentapi.domain.model.PaymentPage search(
                String merchantId, com.example.psp.paymentapi.domain.model.PaymentStatus status, int page, int size) {
            throw new UnsupportedOperationException("not needed by this test");
        }
    }

    private static final class RecordingDocumentStore implements DisputeDocumentStore {
        private final List<String> stored = new ArrayList<>();

        @Override
        public DocumentReference store(String disputeId, byte[] documentBytes, String contentType) {
            stored.add(disputeId);
            return new DocumentReference("disputes", disputeId, documentBytes.length, contentType);
        }
    }

    private static final class RecordingEventPublisher implements DisputeEventPublisher {
        private final List<UUID> inlineCalls = new ArrayList<>();
        private final List<UUID> claimCheckedCalls = new ArrayList<>();

        @Override
        public void publishInline(
                UUID disputeId, UUID paymentId, String merchantId, String reason, byte[] documentBytes,
                String contentType) {
            inlineCalls.add(disputeId);
        }

        @Override
        public void publishClaimChecked(
                UUID disputeId, UUID paymentId, String merchantId, String reason, DocumentReference reference) {
            claimCheckedCalls.add(disputeId);
        }
    }
}
