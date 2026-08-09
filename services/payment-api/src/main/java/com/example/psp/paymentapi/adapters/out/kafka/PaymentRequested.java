package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.DomainEvent;
import com.example.psp.common.events.EventEnvelope;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Concrete event for {@code payments.payment-requested.v1} (ADR-0001 topic name, ADR-0002
 * envelope). One flat record: the shared {@link EventEnvelope} plus this event's own domain
 * fields at the top level - there is deliberately no generic {@code payload} field.
 *
 * <p>JSON for M3 (Jackson serializes records natively); Avro codegen replaces this hand-written
 * record in M9 without changing the shape.
 *
 * @param paymentId  also the record key (ADR-0003) - this topic carries exactly one event per
 *                    aggregate, so the key buys even partition spread, not ordering.
 * @param merchantId the owning merchant; NOT the partition key on this topic (see ADR-0003 for
 *                    why {@code payment-requested} is the one exception to the merchantId-key
 *                    default).
 */
public record PaymentRequested(
        EventEnvelope envelope,
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status)
        implements DomainEvent {
}
