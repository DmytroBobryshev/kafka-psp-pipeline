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
 * <p>M21 splits the event's status into two independent things, decided upstream by
 * {@code adapters.in.kafka.PaymentStatusChangedMapper}: whether/how to touch {@code payments.status}
 * ({@code domainStatus}), and what to record in the {@code payment_status_history} trail
 * ({@code rawStatus}, always). Before M21 these were the same value; IPN_RECEIVED/VERIFIED are the
 * first statuses where they diverge - history-only, {@code domainStatus == null}.
 *
 * @param paymentId        the payment this outcome is about.
 * @param domainStatus     the status to apply to {@code payments.status}, or {@code null} - a
 *                         {@code null} means "history-only, do not touch {@code payments.status}"
 *                         (IPN_RECEIVED/VERIFIED); non-null is applied exactly as before M21
 *                         ({@link ApplyPaymentOutcomeUseCase#execute}'s PENDING/absolute-value
 *                         branch).
 * @param rawStatus        the event's own wire status string, always present - what the
 *                         {@code payment_status_history} row's {@code status} column stores,
 *                         verbatim, regardless of {@code domainStatus}.
 * @param providerReference the event's own {@code providerReference}, or {@code null} (blank on
 *                          the wire maps to {@code null} - see the mapper). Stored on the history
 *                          row (V10's nullable column); never applied to {@code payments}.
 * @param eventId          the envelope's own eventId - {@link ApplyPaymentOutcomeUseCase}'s
 *                         status-trail write uses this as the {@code payment_status_history} dedup
 *                         key (V9's UNIQUE constraint); a redelivery of the same event carries the
 *                         same id.
 * @param occurredAt       the envelope's domain event time - when psp-connector says the outcome
 *                         happened, stored verbatim as the status-trail row's {@code occurredAt}
 *                         (NOT when THIS service happens to process it - see
 *                         {@code domain.model.PaymentStatusHistoryEntry}'s javadoc).
 */
public record ApplyPaymentOutcomeCommand(
        UUID paymentId,
        PaymentStatus domainStatus,
        String rawStatus,
        String providerReference,
        UUID eventId,
        Instant occurredAt) {
}
