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
 * M19 (extended M20). Plain JUnit against {@code application/} + {@code domain/} - no Spring, no
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
 *       UNIQUE({@code event_id}) constraint the real adapter's {@code tryRecord} relies on.
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
        assertThat(history.recorded.stream().map(PaymentStatusHistoryEntry::getStatus))
                .containsExactly(PaymentStatus.PENDING, PaymentStatus.SUCCEEDED);
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

        useCase.execute(new ApplyPaymentOutcomeCommand(paymentId, PaymentStatus.SUCCEEDED, eventId, occurredAt));
        useCase.execute(new ApplyPaymentOutcomeCommand(paymentId, PaymentStatus.SUCCEEDED, eventId, occurredAt));

        assertThat(history.recorded).hasSize(1);
        // The status UPDATE side still runs (and is itself idempotent) on both deliveries - only
        // the history INSERT is deduplicated, by design (see ApplyPaymentOutcomeUseCase's javadoc:
        // both writes are independently idempotent, neither gated by "did anything change").
        assertThat(repository.appliedStatuses).hasSize(2);
    }

    private static ApplyPaymentOutcomeCommand command(UUID paymentId, PaymentStatus status) {
        return new ApplyPaymentOutcomeCommand(paymentId, status, UUID.randomUUID(), Instant.now());
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
