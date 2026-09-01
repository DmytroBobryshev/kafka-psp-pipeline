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

/**
 * M19 (extended M20, M21). Plain JUnit against {@code application/} + {@code domain/} - no Spring, no
 * Kafka, no database, same "fakes, not a live broker/container" style as every other use-case test
 * in this module ({@code CreatePaymentUseCaseTest}, {@code MerchantConfigUseCaseTest}). Exercises
 * the properties {@code adapters.in.kafka.PaymentStatusChangedMapper} and
 * {@code ApplyPaymentOutcomeUseCase} exist to demonstrate:
 *
 * <ul>
 *   <li>the event's own {@code SUCCEEDED}/{@code DECLINED} vocabulary maps onto this table's
 *       {@code SUCCEEDED}/{@code FAILED} vocabulary, not a 1:1 copy;
 *   <li>applying the same terminal outcome twice is a no-op the second time - idempotent by
 *       construction, because {@link RecordingRepository#updateStatus} models the same
 *       absolute-value {@code UPDATE} the real Postgres adapter runs, not an increment or append;
 *   <li>M20: a {@code PENDING} outcome NEVER downgrades a terminal ({@code SUCCEEDED}/
 *       {@code FAILED}) status - {@link RecordingRepository#applyPendingStatus} models the same
 *       {@code WHERE status = CREATED} conditional {@code UPDATE} the real adapter runs, so a
 *       late-replayed {@code PENDING} arriving after the payment has already resolved is a no-op;
 *   <li>M20: a redelivered event (same {@code eventId}) records at most one
 *       {@code payment_status_history} row - {@link RecordingHistoryRepository} models the table's
 *       UNIQUE({@code event_id}) constraint the real adapter's {@code tryRecord} relies on;
 *   <li>M21: IPN_RECEIVED/VERIFIED are history-only - a {@code null} {@code domainStatus} means
 *       {@code payments.status} is never touched, while the raw wire status string and
 *       providerReference still land in {@code payment_status_history}.
 * </ul>
 */
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
        // The mapping itself (event "DECLINED" -> PaymentStatus.FAILED) lives in
        // adapters.in.kafka.PaymentStatusChangedMapper, one hexagon layer further out than this
        // use case - so this test drives the use case directly with the ALREADY-MAPPED
        // PaymentStatus the listener would have produced, and asserts on the one property that
        // belongs at THIS layer: applying FAILED is exactly as idempotent as applying SUCCEEDED,
        // and PaymentStatus has no DECLINED constant to apply in the first place (a compile-time
        // guarantee, not just a runtime one - see PaymentStatus.java).
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

        // Two calls happened (redelivery is possible and expected), but both converge on the
        // exact same absolute value - not two different rows, not a doubled effect.
        assertThat(repository.appliedStatuses).containsExactly(PaymentStatus.SUCCEEDED, PaymentStatus.SUCCEEDED);
        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.SUCCEEDED);
    }

    @Test
    void anUnknownPaymentIdIsANoOpNotAFailure() {
        // See domain.port.PaymentRepository#updateStatus's javadoc: an id this table has never
        // seen is a no-op, never a thrown/reported error - the event that drives this call can
        // only ever name a payment this service itself created.
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());

        useCase.execute(command(UUID.randomUUID(), PaymentStatus.SUCCEEDED));

        assertThat(repository.appliedStatuses).hasSize(1);
    }

    @Test
    void pendingAppliesWhenPaymentIsStillCreated() {
        // A fresh payment has never had updateStatus/applyPendingStatus called on it -
        // RecordingRepository treats "never touched" the same as CREATED (the real row's actual
        // initial value), matching the real adapter's WHERE status = CREATED guard.
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.PENDING));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.PENDING);
    }

    @Test
    void pendingDoesNotDowngradeSucceeded() {
        // M20's core guard: psp-connector's PENDING publish happens BEFORE its provider call, so
        // a redelivery of that PENDING record can arrive AFTER the payment has already resolved
        // (events are ordered per-merchant partition, but a redelivery is a replay, not a
        // same-order guarantee). The late PENDING must not erase the terminal outcome.
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
        // A redelivery of the exact same payments.payment-status-changed.v1 record (same
        // envelope.eventId) must not duplicate a payment_status_history row - the table's
        // UNIQUE(event_id) constraint (V9) is the authority; RecordingHistoryRepository models it.
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
        // The status UPDATE side still runs (and is itself idempotent) on both deliveries - only
        // the history INSERT is deduplicated, by design (see ApplyPaymentOutcomeUseCase's javadoc:
        // both writes are independently idempotent, neither gated by "did anything change").
        assertThat(repository.appliedStatuses).hasSize(2);
    }

    @Test
    void ipnReceivedIsHistoryOnlyAndNeverTouchesPaymentsStatus() {
        // M21 stage 3: history-only - domainStatus is null (see
        // adapters.in.kafka.PaymentStatusChangedMapper's javadoc for why), so payments.status must
        // stay completely untouched, while the raw status string and providerReference still land
        // in payment_status_history.
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
        // The realistic sequence: IPN_RECEIVED and VERIFIED (history-only) precede the terminal
        // SUCCEEDED - three history rows total, but exactly one payments.status write.
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
        // M22: EXPIRED's guard ALSO accepts PENDING - the second FROM state PENDING's own guard
        // does not need (applyPendingStatus only ever applies FROM CREATED).
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
        // A redelivered/re-swept EXPIRED (the scheduler's deterministic eventId means a second
        // sweep republishes the same event) must not somehow re-apply on top of itself in a way
        // that breaks the guard - status stays EXPIRED, no exception, no double effect.
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository, new RecordingHistoryRepository());
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(paymentId, PaymentStatus.EXPIRED));
        useCase.execute(command(paymentId, PaymentStatus.EXPIRED));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.EXPIRED);
    }

    @Test
    void aLateSucceededStillOverwritesAnExpiredPayment() {
        // The one deliberate exception to the "never downgrade" family above: a late-arriving
        // terminal provider outcome IS allowed to overwrite EXPIRED - the provider's own answer is
        // authoritative over this service's own expiry guess (updateStatus's unconditional UPDATE,
        // unlike applyExpiredStatus's conditional one - see ApplyPaymentOutcomeUseCase#execute).
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

    /** {@code domainStatus == null} - IPN_RECEIVED/VERIFIED's history-only shape (M21). */
    private static ApplyPaymentOutcomeCommand historyOnlyCommand(
            UUID paymentId, String rawStatus, String providerReference) {
        return new ApplyPaymentOutcomeCommand(
                paymentId, null, rawStatus, providerReference, UUID.randomUUID(), Instant.now());
    }

    /** Fake port: models both writes with the same semantics as the real Postgres adapter. */
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
            // Mirrors the real adapter's "UPDATE ... WHERE status = CREATED": a payment never
            // touched by this fake is, like a real fresh row, implicitly CREATED.
            PaymentStatus current = currentByPaymentId.getOrDefault(paymentId, PaymentStatus.CREATED);
            if (current == PaymentStatus.CREATED) {
                currentByPaymentId.put(paymentId, PaymentStatus.PENDING);
            }
            // else: no-downgrade guard - the real conditional UPDATE simply matches zero rows.
        }

        @Override
        public void applyExpiredStatus(UUID paymentId) {
            // Mirrors the real adapter's "UPDATE ... WHERE status IN (CREATED, PENDING)" - same
            // "never touched = implicitly CREATED" convention as applyPendingStatus above.
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

    /** Fake port: models the table's UNIQUE(event_id) constraint the real adapter's tryRecord relies on. */
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
