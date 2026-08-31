package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.exception.MerchantNotEligibleException;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import com.example.psp.paymentapi.domain.model.MerchantView;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.MerchantViewRepository;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The single use case: create a payment, persist it, publish that it happened.
 *
 * <p>{@code application/} orchestrates ports and MAY use Spring annotations, but never imports an
 * adapter type directly (ADR-0007) - {@link PaymentRepository} and {@link PaymentEventPublisher}
 * are injected as interfaces; which concrete adapter backs them (Postgres, outbox - M6) is a
 * {@code config/}/component-scanning wiring concern.
 */
@Service
public class CreatePaymentUseCase {

    private final PaymentRepository paymentRepository;
    private final MerchantViewRepository merchantViewRepository;
    private final PaymentEventPublisher paymentEventPublisher;

    public CreatePaymentUseCase(
            PaymentRepository paymentRepository,
            MerchantViewRepository merchantViewRepository,
            PaymentEventPublisher paymentEventPublisher) {
        this.paymentRepository = paymentRepository;
        this.merchantViewRepository = merchantViewRepository;
        this.paymentEventPublisher = paymentEventPublisher;
    }

    /**
     * {@code @Transactional} is the fix, not a formality. Through M3-M5, {@code
     * paymentEventPublisher} was {@code adapters.out.kafka.KafkaPaymentEventPublisher}: a real
     * network call to Kafka, made AFTER {@code paymentRepository.save()} already committed.
     * Postgres and Kafka are two separate systems with no shared transaction, so a crash between
     * those two lines lost the event permanently while the payment row survived - the classic
     * "dual-write problem" (see services/payment-api/README.md's M3 section for the full
     * writeup).
     *
     * <p>M6 closes that gap with the transactional outbox pattern: {@code paymentEventPublisher}
     * is now {@code adapters.out.outbox.OutboxPaymentEventPublisher}, which does no I/O to Kafka
     * at all - it inserts a row into the {@code outbox_event} table via plain JPA, the same
     * {@code DataSource} {@code paymentRepository} uses. Wrapping this method in
     * {@code @Transactional} means both writes join ONE Postgres transaction: either both commit
     * or neither does. There is no longer a window where the payment exists but its event does
     * not. Actual delivery to Kafka happens later and out-of-process, via Debezium reading this
     * table's write-ahead log (infra/compose) - that hand-off is at-least-once and asynchronous,
     * but the write that matters (payment + "intent to publish") is now atomic.
     */
    @Transactional
    public Payment execute(CreatePaymentCommand command) {
        // The merchant-config listener applies a PUT ~1s after it commits, via its own Kafka
        // round trip - a payment attempted inside that window correctly sees "unknown merchant",
        // not a bug in either use case.
        MerchantView merchant =
                merchantViewRepository
                        .findById(command.merchantId())
                        .orElseThrow(() -> MerchantNotEligibleException.unknown(command.merchantId()));
        if (merchant.status() != MerchantStatus.ACTIVE) {
            throw MerchantNotEligibleException.notActive(command.merchantId(), merchant.status());
        }
        // Empty allowedCurrencies is the legacy-projection case (pre-M19 record, or one the
        // schema's own default produced) - fall back to the single payoutCurrency rather than
        // rejecting every payment for a merchant this projection has incomplete data for.
        List<String> allowed =
                merchant.allowedCurrencies().isEmpty()
                        ? List.of(merchant.payoutCurrency())
                        : merchant.allowedCurrencies();
        if (!allowed.contains(command.amount().currency())) {
            throw MerchantNotEligibleException.currencyNotAllowed(
                    command.merchantId(), allowed, command.amount().currency());
        }

        Payment payment = Payment.create(command.merchantId(), command.amount());
        Payment saved = paymentRepository.save(payment);
        paymentEventPublisher.publishPaymentCreated(saved);
        return saved;
    }
}
