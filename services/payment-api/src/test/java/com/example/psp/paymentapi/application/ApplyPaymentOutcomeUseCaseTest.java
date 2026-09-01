package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import com.example.psp.paymentapi.domain.port.PaymentStatusHistoryRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ApplyPaymentOutcomeUseCaseTest {

    @Test
    void succeededMapsToSucceeded() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.SUCCEEDED));

        assertThat(repository.appliedStatuses).containsExactly(PaymentStatus.SUCCEEDED);
        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.SUCCEEDED);
    }

    @Test
    void declinedMapsToFailedNotDeclined() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.FAILED));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void applyingTheSameOutcomeTwiceIsIdempotent() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.SUCCEEDED));
        useCase.execute(command(paymentId, PaymentStatus.SUCCEEDED));

        assertThat(repository.appliedStatuses).containsExactly(PaymentStatus.SUCCEEDED, PaymentStatus.SUCCEEDED);
        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.SUCCEEDED);
    }

    @Test
    void anUnknownPaymentIdIsANoOpNotAFailure() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());

        useCase.execute(command(UUID.randomUUID(), PaymentStatus.SUCCEEDED));

        assertThat(repository.appliedStatuses).hasSize(1);
    }

    @Test
    void pendingAppliesWhenPaymentIsStillCreated() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.PENDING));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.PENDING);
    }

    @Test
    void pendingDoesNotDowngradeSucceeded() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.SUCCEEDED));
        useCase.execute(command(paymentId, PaymentStatus.PENDING));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.SUCCEEDED);
    }

    @Test
    void pendingDoesNotDowngradeFailed() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.FAILED));
        useCase.execute(command(paymentId, PaymentStatus.PENDING));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void everyReceivedEventRecordsOneHistoryRow() {
        RecordingRepository repository = new RecordingRepository();
        RecordingHistoryRepository history = new RecordingHistoryRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, history);
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.PENDING));
        useCase.execute(command(paymentId, PaymentStatus.SUCCEEDED));

        assertThat(history.recorded).hasSize(2);
        // M21: the history row's status is now the raw wire string, not the PaymentStatus enum.
        assertThat(history.recorded.stream().map(PaymentStatusHistoryEntry::getStatus))
                .containsExactly("PENDING", "SUCCEEDED");
    }

    @Test
    void duplicateEventIdInsertsHistoryRowOnlyOnce() {
        RecordingRepository repository = new RecordingRepository();
        RecordingHistoryRepository history = new RecordingHistoryRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, history);
        UUID paymentId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        Instant occurredAt = Instant.now();

        useCase.execute(
                new ApplyPaymentOutcomeCommand(
                        paymentId, PaymentStatus.SUCCEEDED, "SUCCEEDED", null, eventId, occurredAt));
        useCase.execute(
                new ApplyPaymentOutcomeCommand(
                        paymentId, PaymentStatus.SUCCEEDED, "SUCCEEDED", null, eventId, occurredAt));

        assertThat(history.recorded).hasSize(1);
        assertThat(repository.appliedStatuses).hasSize(2);
    }

    @Test
    void ipnReceivedIsHistoryOnlyAndNeverTouchesPaymentsStatus() {
        RecordingRepository repository = new RecordingRepository();
        RecordingHistoryRepository history = new RecordingHistoryRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, history);
        UUID paymentId = UUID.randomUUID();
        String providerReference = UUID.randomUUID().toString();

        useCase.execute(historyOnlyCommand(paymentId, "IPN_RECEIVED", providerReference));

        assertThat(repository.appliedStatuses).isEmpty();
        assertThat(repository.currentStatus(paymentId)).isNull();
        assertThat(history.recorded).hasSize(1);
        assertThat(history.recorded.get(0).getStatus()).isEqualTo("IPN_RECEIVED");
        assertThat(history.recorded.get(0).getProviderReference()).isEqualTo(providerReference);
    }

    @Test
    void verifiedIsHistoryOnlyAndNeverTouchesPaymentsStatus() {
        // M21 stage 4 - same contract as IPN_RECEIVED above.
        RecordingRepository repository = new RecordingRepository();
        RecordingHistoryRepository history = new RecordingHistoryRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, history);
        UUID paymentId = UUID.randomUUID();
        String providerReference = UUID.randomUUID().toString();

        useCase.execute(historyOnlyCommand(paymentId, "VERIFIED", providerReference));

        assertThat(repository.appliedStatuses).isEmpty();
        assertThat(repository.currentStatus(paymentId)).isNull();
        assertThat(history.recorded).hasSize(1);
        assertThat(history.recorded.get(0).getStatus()).isEqualTo("VERIFIED");
        assertThat(history.recorded.get(0).getProviderReference()).isEqualTo(providerReference);
    }

    @Test
    void historyOnlyEventsFollowedByTheTerminalOutcomeStillRecordAllThreeRowsAndOneStatusApply() {
        RecordingRepository repository = new RecordingRepository();
        RecordingHistoryRepository history = new RecordingHistoryRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, history);
        UUID paymentId = UUID.randomUUID();
        String providerReference = UUID.randomUUID().toString();

        useCase.execute(historyOnlyCommand(paymentId, "IPN_RECEIVED", providerReference));
        useCase.execute(historyOnlyCommand(paymentId, "VERIFIED", providerReference));
        useCase.execute(command(paymentId, PaymentStatus.SUCCEEDED));

        assertThat(repository.appliedStatuses).containsExactly(PaymentStatus.SUCCEEDED);
        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(history.recorded).hasSize(3);
    }

    @Test
    void expiredAppliesWhenPaymentIsStillCreated() {
        // M22: EXPIRED's guard accepts CREATED, same as PENDING's.
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.EXPIRED));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.EXPIRED);
    }

    @Test
    void expiredAppliesWhenPaymentIsPending() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.PENDING));
        useCase.execute(command(paymentId, PaymentStatus.EXPIRED));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.EXPIRED);
    }

    @Test
    void expiredNeverDowngradesSucceeded() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.SUCCEEDED));
        useCase.execute(command(paymentId, PaymentStatus.EXPIRED));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.SUCCEEDED);
    }

    @Test
    void expiredNeverDowngradesFailed() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.FAILED));
        useCase.execute(command(paymentId, PaymentStatus.EXPIRED));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void expiredIsANoOpOnceAlreadyExpired() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.EXPIRED));
        useCase.execute(command(paymentId, PaymentStatus.EXPIRED));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.EXPIRED);
    }

    @Test
    void aLateSucceededStillOverwritesAnExpiredPayment() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.EXPIRED));
        useCase.execute(command(paymentId, PaymentStatus.SUCCEEDED));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.SUCCEEDED);
    }

    private static ApplyPaymentOutcomeCommand command(UUID paymentId, PaymentStatus status) {
        return new ApplyPaymentOutcomeCommand(
                paymentId, status, status.name(), null, UUID.randomUUID(), Instant.now());
    }

    private static ApplyPaymentOutcomeCommand historyOnlyCommand(
            UUID paymentId, String rawStatus, String providerReference) {
        return new ApplyPaymentOutcomeCommand(
                paymentId, null, rawStatus, providerReference, UUID.randomUUID(), Instant.now());
    }

    private static final class RecordingRepository implements PaymentRepository {
        private final List<PaymentStatus> appliedStatuses = new ArrayList<>();
        private final Map<UUID, PaymentStatus> currentByPaymentId = new HashMap<>();

        @Override
        public Payment save(Payment payment) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public Optional<Payment> findById(UUID id) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public void updateStatus(UUID paymentId, PaymentStatus status) {
            appliedStatuses.add(status);
            currentByPaymentId.put(paymentId, status);
        }

        @Override
        public void applyPendingStatus(UUID paymentId) {
            PaymentStatus current = currentByPaymentId.getOrDefault(paymentId, PaymentStatus.CREATED);
            if (current == PaymentStatus.CREATED) {
                currentByPaymentId.put(paymentId, PaymentStatus.PENDING);
            }
            // else: no-downgrade guard - the real conditional UPDATE simply matches zero rows.
        }

        @Override
        public void applyExpiredStatus(UUID paymentId) {
            PaymentStatus current = currentByPaymentId.getOrDefault(paymentId, PaymentStatus.CREATED);
            if (current == PaymentStatus.CREATED || current == PaymentStatus.PENDING) {
                currentByPaymentId.put(paymentId, PaymentStatus.EXPIRED);
            }
            // else: no-downgrade guard - the real conditional UPDATE simply matches zero rows.
        }

        @Override
        public List<Payment> findExpirationCandidates(Instant now) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public PaymentPage search(String merchantId, PaymentStatus status, int page, int size) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        private PaymentStatus currentStatus(UUID paymentId) {
            return currentByPaymentId.get(paymentId);
        }
    }

    private static final class RecordingHistoryRepository implements PaymentStatusHistoryRepository {
        private final List<PaymentStatusHistoryEntry> recorded = new ArrayList<>();
        private final Set<UUID> seenEventIds = new HashSet<>();

        @Override
        public boolean tryRecord(PaymentStatusHistoryEntry entry) {
            if (!seenEventIds.add(entry.getEventId())) {
                return false;
            }
            recorded.add(entry);
            return true;
        }

        @Override
        public List<PaymentStatusHistoryEntry> findByPaymentId(UUID paymentId) {
            return recorded.stream().filter(e -> e.getPaymentId().equals(paymentId)).toList();
        }
    }
}
