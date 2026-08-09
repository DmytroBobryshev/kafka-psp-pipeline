package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import org.springframework.stereotype.Service;

/**
 * The single use case: create a payment, persist it, publish that it happened.
 *
 * <p>{@code application/} orchestrates ports and MAY use Spring annotations, but never imports an
 * adapter type directly (ADR-0007) - {@link PaymentRepository} and {@link PaymentEventPublisher}
 * are injected as interfaces; which concrete adapter backs them (Postgres, Kafka - M3) is a
 * {@code config/} wiring concern.
 */
@Service
public class CreatePaymentUseCase {

    private final PaymentRepository paymentRepository;
    private final PaymentEventPublisher paymentEventPublisher;

    public CreatePaymentUseCase(
            PaymentRepository paymentRepository, PaymentEventPublisher paymentEventPublisher) {
        this.paymentRepository = paymentRepository;
        this.paymentEventPublisher = paymentEventPublisher;
    }

    public Payment execute(CreatePaymentCommand command) {
        Payment payment = Payment.create(command.merchantId(), command.amount());

        // Persist-then-publish: Postgres and Kafka are two separate systems with no shared
        // transaction. If this process crashes AFTER paymentRepository.save() commits but
        // BEFORE paymentEventPublisher.publishPaymentCreated() succeeds, the payment row exists
        // durably while payments.payment-requested.v1 never receives the event - the classic
        // "dual-write problem". M6 fixes this with a transactional outbox: the event row is
        // written to an outbox table in the SAME Postgres transaction as the payment row, and a
        // separate relay process publishes outbox rows to Kafka, so the write and the "intent to
        // publish" become atomic even though the publish itself still happens later. NOT
        // implemented here on purpose - M3's scope is producer mechanics (acks, batching,
        // idempotence), and conflating it with the outbox pattern would muddy both lessons. The
        // gap is real: kill the process between these two lines and the row survives, the event
        // doesn't (see services/payment-api/README.md for how M6 will close it).
        Payment saved = paymentRepository.save(payment);
        paymentEventPublisher.publishPaymentCreated(saved);
        return saved;
    }
}
