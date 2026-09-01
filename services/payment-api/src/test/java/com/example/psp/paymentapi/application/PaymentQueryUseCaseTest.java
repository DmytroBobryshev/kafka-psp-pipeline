package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentHistoryItem;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import com.example.psp.paymentapi.domain.model.Refund;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import com.example.psp.paymentapi.domain.port.PaymentStatusHistoryRepository;
import com.example.psp.paymentapi.domain.port.RefundRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * M20. Plain JUnit against {@code application/} + {@code domain/} - no Spring, no Kafka, no
 * database, same "fakes, not a live broker/container" style as every other use-case test in this
 * module. Exercises {@link PaymentQueryUseCase#history} - the assembly
 * {@code domain.model.PaymentHistoryItem}'s javadoc describes: exactly one synthetic
 * {@code CREATED} entry from the payment row, plus every recorded {@code payment_status_history}
 * row, merged and sorted {@code occurredAt} ascending.
 */
class PaymentQueryUseCaseTest {

    @Test
    void historyAlwaysStartsWithASyntheticCreatedEntryFromThePaymentRow() {
        Instant createdAt = Instant.parse("2026-08-01T10:00:00Z");
        UUID paymentId = UUID.randomUUID();
        FakePaymentRepository payments = new FakePaymentRepository();
        payments.put(paymentWithStatus(paymentId, createdAt, PaymentStatus.CREATED));
        PaymentQueryUseCase useCase =
                new PaymentQueryUseCase(payments, new UnsupportedRefundRepository(), new FakeHistoryRepository());

        List<PaymentHistoryItem> history = useCase.history(paymentId);

        assertThat(history).hasSize(1);
        PaymentHistoryItem created = history.get(0);
        assertThat(created.status()).isEqualTo("CREATED");
        assertThat(created.occurredAt()).isEqualTo(createdAt);
        assertThat(created.eventId()).isNull();
        assertThat(created.source()).isEqualTo("payment-api");
        assertThat(created.providerReference()).isNull();
    }

    @Test
    void mergesTheStatusTrailAndSortsByOccurredAtAscendingRegardlessOfInsertionOrder() {
        Instant createdAt = Instant.parse("2026-08-01T10:00:00Z");
        Instant pendingAt = createdAt.plus(1, ChronoUnit.SECONDS);
        Instant succeededAt = createdAt.plus(2, ChronoUnit.SECONDS);
        UUID paymentId = UUID.randomUUID();
        UUID pendingEventId = UUID.randomUUID();
        UUID succeededEventId = UUID.randomUUID();

        FakePaymentRepository payments = new FakePaymentRepository();
        payments.put(paymentWithStatus(paymentId, createdAt, PaymentStatus.SUCCEEDED));
        FakeHistoryRepository history = new FakeHistoryRepository();
        // Inserted out of order on purpose - the use case must sort, not trust storage order.
        history.add(PaymentStatusHistoryEntry.reconstitute(
                UUID.randomUUID(),
                paymentId,
                "SUCCEEDED",
                "provider-ref-1",
                succeededEventId,
                succeededAt,
                succeededAt));
        history.add(PaymentStatusHistoryEntry.reconstitute(
                UUID.randomUUID(), paymentId, "PENDING", null, pendingEventId, pendingAt, pendingAt));

        PaymentQueryUseCase useCase =
                new PaymentQueryUseCase(payments, new UnsupportedRefundRepository(), history);

        List<PaymentHistoryItem> result = useCase.history(paymentId);

        assertThat(result).extracting(PaymentHistoryItem::status)
                .containsExactly("CREATED", "PENDING", "SUCCEEDED");
        assertThat(result).extracting(PaymentHistoryItem::occurredAt)
                .containsExactly(createdAt, pendingAt, succeededAt);
        // Every non-CREATED entry carries its eventId and is attributed to psp-connector, the
        // sole publisher of payments.payment-status-changed.v1.
        assertThat(result.get(1).eventId()).isEqualTo(pendingEventId);
        assertThat(result.get(1).source()).isEqualTo("psp-connector");
        assertThat(result.get(1).providerReference()).isNull();
        assertThat(result.get(2).eventId()).isEqualTo(succeededEventId);
        assertThat(result.get(2).source()).isEqualTo("psp-connector");
        assertThat(result.get(2).providerReference()).isEqualTo("provider-ref-1");
    }

    @Test
    void historyIncludesNonTerminalTrailStatusesVerbatim() {
        // M21: IPN_RECEIVED/VERIFIED have no PaymentStatus equivalent - the read side passes them
        // through as opaque strings, exactly like every other status, providerReference included.
        Instant createdAt = Instant.parse("2026-08-01T10:00:00Z");
        Instant ipnAt = createdAt.plus(1, ChronoUnit.SECONDS);
        UUID paymentId = UUID.randomUUID();
        UUID ipnEventId = UUID.randomUUID();
        String providerReference = UUID.randomUUID().toString();

        FakePaymentRepository payments = new FakePaymentRepository();
        payments.put(paymentWithStatus(paymentId, createdAt, PaymentStatus.PENDING));
        FakeHistoryRepository history = new FakeHistoryRepository();
        history.add(PaymentStatusHistoryEntry.reconstitute(
                UUID.randomUUID(), paymentId, "IPN_RECEIVED", providerReference, ipnEventId, ipnAt, ipnAt));

        PaymentQueryUseCase useCase =
                new PaymentQueryUseCase(payments, new UnsupportedRefundRepository(), history);

        List<PaymentHistoryItem> result = useCase.history(paymentId);

        assertThat(result).hasSize(2);
        PaymentHistoryItem ipn = result.get(1);
        assertThat(ipn.status()).isEqualTo("IPN_RECEIVED");
        assertThat(ipn.eventId()).isEqualTo(ipnEventId);
        assertThat(ipn.providerReference()).isEqualTo(providerReference);
        assertThat(ipn.source()).isEqualTo("psp-connector");
    }

    @Test
    void unknownPaymentIdThrowsNoSuchElement() {
        PaymentQueryUseCase useCase =
                new PaymentQueryUseCase(
                        new FakePaymentRepository(), new UnsupportedRefundRepository(), new FakeHistoryRepository());

        assertThatThrownBy(() -> useCase.history(UUID.randomUUID()))
                .isInstanceOf(NoSuchElementException.class);
    }

    private static Payment paymentWithStatus(UUID id, Instant createdAt, PaymentStatus status) {
        return Payment.reconstitute(
                id, "merchant-1", Money.of(BigDecimal.TEN, "EUR"), status, createdAt, null);
    }

    private static final class FakePaymentRepository implements PaymentRepository {
        private final Map<UUID, Payment> byId = new HashMap<>();

        void put(Payment payment) {
            byId.put(payment.getId(), payment);
        }

        @Override
        public Payment save(Payment payment) {
            throw new UnsupportedOperationException("not exercised by this test");
        }

        @Override
        public Optional<Payment> findById(UUID id) {
            return Optional.ofNullable(byId.get(id));
        }

        @Override
        public void updateStatus(UUID paymentId, PaymentStatus status) {
            throw new UnsupportedOperationException("not exercised by this test");
        }

        @Override
        public void applyPendingStatus(UUID paymentId) {
            throw new UnsupportedOperationException("not exercised by this test");
        }

        @Override
        public PaymentPage search(String merchantId, PaymentStatus status, int page, int size) {
            throw new UnsupportedOperationException("not exercised by this test");
        }
    }

    private static final class FakeHistoryRepository implements PaymentStatusHistoryRepository {
        private final List<PaymentStatusHistoryEntry> entries = new ArrayList<>();

        void add(PaymentStatusHistoryEntry entry) {
            entries.add(entry);
        }

        @Override
        public boolean tryRecord(PaymentStatusHistoryEntry entry) {
            throw new UnsupportedOperationException("not exercised by this test");
        }

        @Override
        public List<PaymentStatusHistoryEntry> findByPaymentId(UUID paymentId) {
            return entries.stream().filter(e -> e.getPaymentId().equals(paymentId)).toList();
        }
    }

    /** {@code history()} never touches refunds - every method throws if that assumption breaks. */
    private static final class UnsupportedRefundRepository implements RefundRepository {
        @Override
        public Refund save(Refund refund) {
            throw new UnsupportedOperationException("not exercised by this test");
        }

        @Override
        public BigDecimal sumRequestedAmount(UUID paymentId) {
            throw new UnsupportedOperationException("not exercised by this test");
        }

        @Override
        public List<Refund> findByPaymentId(UUID paymentId) {
            throw new UnsupportedOperationException("not exercised by this test");
        }
    }
}
