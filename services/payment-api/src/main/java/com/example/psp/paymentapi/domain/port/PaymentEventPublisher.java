package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Payment;

/**
 * Outbound port for publishing payment domain events. Implemented by {@code adapters/out/kafka}
 * - the domain never imports {@code org.apache.kafka} or Spring Kafka directly (ADR-0007).
 *
 * <p>M1 ships an in-memory/no-op adapter only; real publishing of {@code payments.requested}
 * arrives in M3.
 */
public interface PaymentEventPublisher {

    void publishPaymentCreated(Payment payment);
}
