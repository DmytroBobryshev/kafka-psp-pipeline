package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import org.springframework.stereotype.Service;

/**
 * The single M1 use case: create a payment, persist it, publish that it happened.
 *
 * <p>{@code application/} orchestrates ports and MAY use Spring annotations, but never imports an
 * adapter type directly (ADR-0007) - {@link PaymentRepository} and {@link PaymentEventPublisher}
 * are injected as interfaces; which concrete adapter backs them is a {@code config/} wiring
 * concern.
 *
 * <p>M1 keeps this intentionally thin (no validation beyond what {@code Money}/{@code Payment}
 * already enforce, no idempotency, no outbox). Real business rules land in M3+.
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
        Payment saved = paymentRepository.save(payment);
        paymentEventPublisher.publishPaymentCreated(saved);
        return saved;
    }
}
