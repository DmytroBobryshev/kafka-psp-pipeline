package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Payment;

/**
 * Outbound port for publishing payment domain events. The domain never imports
 * {@code org.apache.kafka}, Spring Kafka, or JPA directly (ADR-0007) - only this interface.
 *
 * <p>M1 shipped an in-memory/no-op adapter. M3 added {@code adapters.out.kafka.
 * KafkaPaymentEventPublisher}, calling Kafka directly after the payment row committed (the
 * dual-write problem). M6 replaces it with {@code adapters.out.outbox.OutboxPaymentEventPublisher}
 * - "publish" now means "write an outbox row in the same Postgres transaction as the payment";
 * actual Kafka delivery happens later, out of process, via Debezium (see
 * services/payment-api/README.md's M6 section). The port's name and signature didn't need to
 * change for that swap - only which adapter implements it, which is exactly the point of
 * depending on an interface here instead of a concrete adapter type.
 */
public interface PaymentEventPublisher {

    void publishPaymentCreated(Payment payment);
}
