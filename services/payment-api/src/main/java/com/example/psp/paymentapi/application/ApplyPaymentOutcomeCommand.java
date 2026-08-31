package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.time.Instant;
import java.util.UUID;

/**
 * Application-layer input model for {@link ApplyPaymentOutcomeUseCase} (M19; {@code eventId}/
 * {@code occurredAt} added M20). Deliberately separate from the inbound Kafka event
 * ({@code com.example.psp.common.events.avro.PaymentStatusChanged}) - the use case never depends
 * on an Avro wire type, only on this plain command, the same "adapter maps to a command, use case
 * never sees the wire shape" rule every other use case in this module already follows (compare
 * {@link CreatePaymentCommand}, driven by the web adapter instead of a Kafka one).
 *
 * @param eventId    the envelope's own eventId - {@link ApplyPaymentOutcomeUseCase}'s status-trail
 *                   write uses this as the {@code payment_status_history} dedup key (V9's UNIQUE
 *                   constraint); a redelivery of the same event carries the same id.
 * @param occurredAt the envelope's domain event time - when psp-connector says the outcome
 *                   happened, stored verbatim as the status-trail row's {@code occurredAt} (NOT
 *                   when THIS service happens to process it - see
 *                   {@code domain.model.PaymentStatusHistoryEntry}'s javadoc).
 */
public record ApplyPaymentOutcomeCommand(UUID paymentId, PaymentStatus status, UUID eventId, Instant occurredAt) {
}
