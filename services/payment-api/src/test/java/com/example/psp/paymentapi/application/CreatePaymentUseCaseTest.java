package com.example.psp.paymentapi.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.psp.paymentapi.domain.model.MerchantPage;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.model.MerchantView;
import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.port.MerchantViewRepository;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
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
        CreatePaymentUseCase useCase =
                new CreatePaymentUseCase(repository, activeMerchant("merchant-1"), publisher);

        Payment result =
                useCase.execute(new CreatePaymentCommand("merchant-1", new Money(BigDecimal.TEN, "EUR")));

        assertThat(result.getStatus()).isEqualTo(PaymentStatus.CREATED);
        assertThat(repository.findById(result.getId())).contains(result);
        assertThat(publisher.publishedCount.get()).isEqualTo(1);
        assertThat(publisher.lastPublished).isEqualTo(result);
    }

    @Test
    void rejectsAPaymentForAMerchantAbsentFromTheProjection() {
        InMemoryFakeRepository repository = new InMemoryFakeRepository();
        CreatePaymentUseCase useCase =
                new CreatePaymentUseCase(repository, new StubMerchantViewRepository(null), new RecordingFakePublisher());

        assertThatThrownBy(
                        () ->
                                useCase.execute(
                                        new CreatePaymentCommand("merchant-unknown", new Money(BigDecimal.TEN, "EUR"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown merchant merchant-unknown");
        assertThat(repository.store).isEmpty();
    }

    @Test
    void rejectsAPaymentForASuspendedMerchant() {
        InMemoryFakeRepository repository = new InMemoryFakeRepository();
        MerchantView suspended =
                new MerchantView(
                        "merchant-2",
                        "Suspended Co",
                        MerchantStatus.SUSPENDED,
                        "EUR",
                        List.of("EUR"),
                        null,
                        1500,
                        900,
                        Instant.now());
        CreatePaymentUseCase useCase =
                new CreatePaymentUseCase(
                        repository, new StubMerchantViewRepository(suspended), new RecordingFakePublisher());

        assertThatThrownBy(
                        () ->
                                useCase.execute(
                                        new CreatePaymentCommand("merchant-2", new Money(BigDecimal.TEN, "EUR"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merchant-2 is not active (status=SUSPENDED)");
    }

    @Test
    void acceptsAPaymentInAnyOfTheMerchantsAllowedCurrencies() {
        InMemoryFakeRepository repository = new InMemoryFakeRepository();
        MerchantView merchant =
                new MerchantView(
                        "merchant-3",
                        "Multi-currency Co",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR", "USD", "GBP"),
                        null,
                        1500,
                        900,
                        Instant.now());
        CreatePaymentUseCase useCase =
                new CreatePaymentUseCase(
                        repository, new StubMerchantViewRepository(merchant), new RecordingFakePublisher());

        // GBP is allowed even though it is not the payoutCurrency (EUR) - the gate checks
        // membership in the whole set, not equality with the single settlement currency.
        Payment result =
                useCase.execute(new CreatePaymentCommand("merchant-3", new Money(BigDecimal.TEN, "GBP")));

        assertThat(result.getAmount().currency()).isEqualTo("GBP");
    }

    @Test
    void rejectsAPaymentInACurrencyNotInTheMerchantsAllowedList() {
        InMemoryFakeRepository repository = new InMemoryFakeRepository();
        MerchantView merchant =
                new MerchantView(
                        "merchant-4",
                        "Eur-Usd Co",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR", "USD"),
                        null,
                        1500,
                        900,
                        Instant.now());
        CreatePaymentUseCase useCase =
                new CreatePaymentUseCase(
                        repository, new StubMerchantViewRepository(merchant), new RecordingFakePublisher());

        assertThatThrownBy(
                        () ->
                                useCase.execute(
                                        new CreatePaymentCommand("merchant-4", new Money(BigDecimal.TEN, "GBP"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merchant-4 accepts only [EUR, USD] (got GBP)");
    }

    @Test
    void legacyEmptyAllowedCurrenciesFallsBackToPayoutCurrency() {
        InMemoryFakeRepository repository = new InMemoryFakeRepository();
        MerchantView legacy =
                new MerchantView(
                        "merchant-5",
                        "Legacy Co",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of(),
                        null,
                        1500,
                        900,
                        Instant.now());
        CreatePaymentUseCase useCase =
                new CreatePaymentUseCase(
                        repository, new StubMerchantViewRepository(legacy), new RecordingFakePublisher());

        Payment accepted =
                useCase.execute(new CreatePaymentCommand("merchant-5", new Money(BigDecimal.TEN, "EUR")));
        assertThat(accepted.getAmount().currency()).isEqualTo("EUR");

        assertThatThrownBy(
                        () ->
                                useCase.execute(
                                        new CreatePaymentCommand("merchant-5", new Money(BigDecimal.TEN, "USD"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merchant-5 accepts only [EUR] (got USD)");
    }

    private static MerchantViewRepository activeMerchant(String merchantId) {
        return new StubMerchantViewRepository(
                new MerchantView(
                        merchantId,
                        "Test Merchant",
                        MerchantStatus.ACTIVE,
                        "EUR",
                        List.of("EUR"),
                        null,
                        1500,
                        900,
                        Instant.now()));
    }

    /** Fake port: returns the fixed {@link MerchantView} passed at construction, or empty. */
    private static final class StubMerchantViewRepository implements MerchantViewRepository {
        private final MerchantView view;

        private StubMerchantViewRepository(MerchantView view) {
            this.view = view;
        }

        @Override
        public void upsert(MerchantView view) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public void delete(String merchantId) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }

        @Override
        public Optional<MerchantView> findById(String merchantId) {
            return Optional.ofNullable(view);
        }

        @Override
        public MerchantPage search(MerchantStatus status, int page, int size) {
            throw new UnsupportedOperationException("not exercised by this use case");
        }
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

        @Override
        public void applyPendingStatus(UUID paymentId) {
            throw new UnsupportedOperationException(
                    "applyPendingStatus() is not part of the create-payment use case under test");
        }

        @Override
        public void applyExpiredStatus(UUID paymentId) {
            throw new UnsupportedOperationException(
                    "applyExpiredStatus() is not part of the create-payment use case under test");
        }

        @Override
        public List<Payment> findExpirationCandidates(java.time.Instant now) {
            throw new UnsupportedOperationException(
                    "findExpirationCandidates() is not part of the create-payment use case under test");
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
