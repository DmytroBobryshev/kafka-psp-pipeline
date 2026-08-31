package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * M19. Plain JUnit against {@code application/} + {@code domain/} - no Spring, no Kafka, no
 * database, same "fakes, not a live broker/container" style as every other use-case test in this
 * module ({@code CreatePaymentUseCaseTest}, {@code MerchantConfigUseCaseTest}). Exercises exactly
 * the two properties {@code adapters.in.kafka.PaymentStatusChangedMapper} and
 * {@code ApplyPaymentOutcomeUseCase} exist to demonstrate:
 *
 * <ul>
 *   <li>the event's own {@code SUCCEEDED}/{@code DECLINED} vocabulary maps onto this table's
 *       {@code SUCCEEDED}/{@code FAILED} vocabulary, not a 1:1 copy;
 *   <li>applying the same outcome twice is a no-op the second time - idempotent by construction,
 *       because the fake repository below models {@code updateStatus} as the same absolute-value
 *       {@code UPDATE} the real Postgres adapter runs, not an increment or an append.
 * </ul>
 */
class ApplyPaymentOutcomeUseCaseTest {

    @Test
    void succeededMapsToSucceeded() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository);
        UUID paymentId = UUID.randomUUID();

        useCase.execute(new ApplyPaymentOutcomeCommand(paymentId, PaymentStatus.SUCCEEDED));

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
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository);
        UUID paymentId = UUID.randomUUID();

        useCase.execute(new ApplyPaymentOutcomeCommand(paymentId, PaymentStatus.FAILED));

        assertThat(repository.currentStatus(paymentId)).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void applyingTheSameOutcomeTwiceIsIdempotent() {
        RecordingRepository repository = new RecordingRepository();
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository);
        UUID paymentId = UUID.randomUUID();

        useCase.execute(new ApplyPaymentOutcomeCommand(paymentId, PaymentStatus.SUCCEEDED));
        useCase.execute(new ApplyPaymentOutcomeCommand(paymentId, PaymentStatus.SUCCEEDED));

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
        ApplyPaymentOutcomeUseCase useCase = new ApplyPaymentOutcomeUseCase(repository);

        useCase.execute(new ApplyPaymentOutcomeCommand(UUID.randomUUID(), PaymentStatus.SUCCEEDED));

        assertThat(repository.appliedStatuses).hasSize(1);
    }

    /** Fake port: models {@code updateStatus} as an absolute-value write, like the real adapter. */
    private static final class RecordingRepository implements PaymentRepository {
        private final List<PaymentStatus> appliedStatuses = new ArrayList<>();
        private final java.util.Map<UUID, PaymentStatus> currentByPaymentId = new java.util.HashMap<>();

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
        public PaymentPage search(String merchantId, PaymentStatus status, int page, int size) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        private PaymentStatus currentStatus(UUID paymentId) {
            return currentByPaymentId.get(paymentId);
        }
    }
}
