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
        assertThat(repository.recorded).allSatisfy(e -> {
            assertThat(e.getRefundId()).isEqualTo(refundId);
            assertThat(e.getPaymentId()).isEqualTo(paymentId);
        });
    }

    @Test
    void fundsReservedAndFailedRowsCarryNoProviderReference() {
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
