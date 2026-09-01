package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.paymentapi.domain.model.RefundStatusHistoryEntry;
import com.example.psp.paymentapi.domain.port.RefundStatusHistoryRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * M23. Plain JUnit against {@code application/} + {@code domain/} - no Spring, no Kafka, no
 * database, same "fakes, not a live broker/container" style as {@code ApplyPaymentOutcomeUseCaseTest}.
 * Exercises what {@link RecordRefundHistoryUseCase} exists to demonstrate: every one of the four
 * refund-trail listeners does exactly one thing (an unconditional history-only insert, no state
 * machine anywhere), and a redelivered event (same {@code eventId}) records at most one row - the
 * table's UNIQUE({@code event_id}) constraint (V12) is the authority, {@link RecordingRepository}
 * models it.
 */
class RecordRefundHistoryUseCaseTest {

    @Test
    void recordsOneHistoryRowPerCommand() {
        RecordingRepository repository = new RecordingRepository();
        RecordRefundHistoryUseCase useCase = new RecordRefundHistoryUseCase(repository);
        UUID refundId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(refundId, paymentId, "PENDING", null));
        useCase.execute(command(refundId, paymentId, "IPN_RECEIVED", "provider-ref-1"));
        useCase.execute(command(refundId, paymentId, "VERIFIED", "provider-ref-1"));
        useCase.execute(command(refundId, paymentId, "COMPLETED", "provider-ref-1"));

        assertThat(repository.recorded).hasSize(4);
        assertThat(repository.recorded.stream().map(RefundStatusHistoryEntry::getStatus))
                .containsExactly("PENDING", "IPN_RECEIVED", "VERIFIED", "COMPLETED");
        // Every row carries the same refundId/paymentId regardless of which of the four listeners
        // produced it - the use case never branches on status beyond storing it verbatim.
        assertThat(repository.recorded).allSatisfy(e -> {
            assertThat(e.getRefundId()).isEqualTo(refundId);
            assertThat(e.getPaymentId()).isEqualTo(paymentId);
        });
    }

    @Test
    void fundsReservedAndFailedRowsCarryNoProviderReference() {
        // Neither refunds.funds-reserved.v1 nor refunds.refund-failed.v1 carries a
        // providerReference field - the listeners pass null through, and this use case never
        // invents one.
        RecordingRepository repository = new RecordingRepository();
        RecordRefundHistoryUseCase useCase = new RecordRefundHistoryUseCase(repository);
        UUID refundId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        useCase.execute(command(refundId, paymentId, "FUNDS_RESERVED", null));
        useCase.execute(command(refundId, paymentId, "FAILED", null));

        assertThat(repository.recorded).extracting(RefundStatusHistoryEntry::getProviderReference)
                .containsExactly((String) null, null);
        assertThat(repository.recorded).extracting(RefundStatusHistoryEntry::getStatus)
                .containsExactly("FUNDS_RESERVED", "FAILED");
    }

    @Test
    void duplicateEventIdInsertsHistoryRowOnlyOnce() {
        // A redelivery of the exact same upstream event (same envelope.eventId) must not
        // duplicate a refund_status_history row - the table's UNIQUE(event_id) constraint (V12)
        // is the authority, RecordingRepository models it.
        RecordingRepository repository = new RecordingRepository();
        RecordRefundHistoryUseCase useCase = new RecordRefundHistoryUseCase(repository);
        UUID refundId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        Instant occurredAt = Instant.now();

        useCase.execute(new RecordRefundHistoryCommand(refundId, paymentId, "COMPLETED", "ref-1", eventId, occurredAt));
        useCase.execute(new RecordRefundHistoryCommand(refundId, paymentId, "COMPLETED", "ref-1", eventId, occurredAt));

        assertThat(repository.recorded).hasSize(1);
    }

    private static RecordRefundHistoryCommand command(
            UUID refundId, UUID paymentId, String status, String providerReference) {
        return new RecordRefundHistoryCommand(
                refundId, paymentId, status, providerReference, UUID.randomUUID(), Instant.now());
    }

    /** Fake port: models the table's UNIQUE(event_id) constraint the real adapter's tryRecord relies on. */
    private static final class RecordingRepository implements RefundStatusHistoryRepository {
        private final List<RefundStatusHistoryEntry> recorded = new ArrayList<>();
        private final Set<UUID> seenEventIds = new HashSet<>();

        @Override
        public boolean tryRecord(RefundStatusHistoryEntry entry) {
            if (!seenEventIds.add(entry.getEventId())) {
                return false;
            }
            recorded.add(entry);
            return true;
        }

        @Override
        public List<RefundStatusHistoryEntry> findByRefundId(UUID refundId) {
            return recorded.stream().filter(e -> e.getRefundId().equals(refundId)).toList();
        }
    }
}
