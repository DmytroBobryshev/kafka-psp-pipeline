package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Plain JUnit against {@code application/} + {@code domain/} - no Spring context, no Kafka, no
 * database. This is exactly the ArchUnit-enforced payoff described in ADR-0007: the interesting
 * logic is testable without any framework in the loop.
 */
class CreatePaymentUseCaseTest {

    @Test
    void createsPersistsAndPublishesAPayment() {
        InMemoryFakeRepository repository = new InMemoryFakeRepository();
        RecordingFakePublisher publisher = new RecordingFakePublisher();
        CreatePaymentUseCase useCase = new CreatePaymentUseCase(repository, publisher);

        Payment result =
                useCase.execute(new CreatePaymentCommand("merchant-1", new Money(BigDecimal.TEN, "EUR")));

        assertThat(result.getStatus()).isEqualTo(PaymentStatus.CREATED);
        assertThat(repository.findById(result.getId())).contains(result);
        assertThat(publisher.publishedCount.get()).isEqualTo(1);
        assertThat(publisher.lastPublished).isEqualTo(result);
    }

    private static final class InMemoryFakeRepository implements PaymentRepository {
        private final Map<UUID, Payment> store = new HashMap<>();

        @Override
        public Payment save(Payment payment) {
            store.put(payment.getId(), payment);
            return payment;
        }

        @Override
        public Optional<Payment> findById(UUID id) {
            return Optional.ofNullable(store.get(id));
        }

        /**
         * Neither of the two methods below is exercised by this test - CreatePaymentUseCase only
         * saves. They are implemented because the port declares them, and they throw rather than
         * fake anything: an in-memory filter/paginate would be a second, untested implementation of
         * PaymentQueryUseCase's contract living in a test file, and a silent no-op updateStatus
         * would let a future test pass while asserting nothing.
         */
        @Override
        public PaymentPage search(String merchantId, PaymentStatus status, int page, int size) {
            throw new UnsupportedOperationException(
                    "search() is not part of the create-payment use case under test");
        }

        @Override
        public void updateStatus(UUID paymentId, PaymentStatus status) {
            throw new UnsupportedOperationException(
                    "updateStatus() is not part of the create-payment use case under test");
        }
    }

    private static final class RecordingFakePublisher implements PaymentEventPublisher {
        private final AtomicInteger publishedCount = new AtomicInteger();
        private Payment lastPublished;

        @Override
        public void publishPaymentCreated(Payment payment) {
            publishedCount.incrementAndGet();
            lastPublished = payment;
        }
    }
}
