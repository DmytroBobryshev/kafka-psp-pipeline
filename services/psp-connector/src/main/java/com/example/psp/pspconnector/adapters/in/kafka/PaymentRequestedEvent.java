package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.common.events.DomainEvent;
import com.example.psp.common.events.EventEnvelope;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * JSON-era wire shape of {@code payments.payment-requested.v1} - the consuming side's mirror of
 * {@code payment-api}'s (also retired) {@code adapters.out.kafka.PaymentRequested} producer
 * record.
 *
 * <p><b>Retired as of M9 Phase 1</b> for the production listener
 * ({@code PaymentRequestedListener} now consumes the generated
 * {@code com.example.psp.common.events.avro.PaymentRequested} Avro class instead - see
 * {@code PaymentRequestedMapper}), same "kept for reference, no {@code @Component}, nothing
 * wires it in" convention as {@code adapters.out.kafka.KafkaPaymentEventPublisher} (M3) on the
 * payment-api side. Still used by the {@code auto-commit-drill} profile's
 * {@code AutoCommitDriftListener}/{@code KafkaAutoCommitDriftConfig} (an M4 experiment, disabled
 * by default) - see those classes' javadoc for why that drill was left on the JSON-era shape
 * rather than updated for M9.
 */
public record PaymentRequestedEvent(
        EventEnvelope envelope,
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status)
        implements DomainEvent {
}
