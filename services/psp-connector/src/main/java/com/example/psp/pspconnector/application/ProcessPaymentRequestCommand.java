package com.example.psp.pspconnector.application;

import com.example.psp.pspconnector.domain.model.Money;
import java.util.UUID;

/**
 * Application-layer input model for {@link ProcessPaymentRequestUseCase}. Deliberately separate
 * from {@code adapters.in.kafka.PaymentRequestedEvent} - the Kafka adapter maps its event onto
 * this command, so the use case never depends on the wire contract (same pattern as
 * {@code payment-api}'s {@code CreatePaymentCommand}).
 *
 * @param causationEventId the inbound event's own {@code EventEnvelope.eventId}. Two distinct
 *                          jobs, both from this one value: (1) becomes this attempt's causation
 *                          link when the outbound status event is published, and (2) is M5
 *                          LEVEL 1's replay/consumer idempotency key - {@code
 *                          ProcessPaymentRequestUseCase} checks {@code
 *                          AttemptLogRepository#existsByInboundEventId} against it BEFORE calling
 *                          the provider, precisely because this id (unlike a provider-minted one)
 *                          is stable across replays, rebalances and offset resets. See that
 *                          class's javadoc.
 * @param traceId           propagated from the inbound envelope (real W3C trace-context
 *                           propagation arrives in M15; until then this just carries the
 *                           upstream id through unchanged).
 * @param correlationId     propagated from the inbound envelope.
 */
public record ProcessPaymentRequestCommand(
        UUID paymentId,
        String merchantId,
        Money amount,
        UUID causationEventId,
        String traceId,
        String correlationId) {
}
