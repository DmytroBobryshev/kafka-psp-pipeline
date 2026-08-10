package com.example.psp.paymentapi.adapters.out.outbox;

import com.example.psp.common.events.DomainEvent;
import com.example.psp.common.events.EventEnvelope;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * The JSON shape written into {@code outbox_event.payload} (M6). Field-for-field identical to
 * the retired {@link com.example.psp.paymentapi.adapters.out.kafka.PaymentRequested} - and, by
 * the same "duplicated per boundary, not shared" convention that record's Javadoc documents,
 * that's deliberate rather than an oversight. This type belongs to the outbox boundary (it never
 * touches Kafka - {@link OutboxPaymentEventPublisher} serializes it straight to a JSONB column),
 * while {@code adapters.out.kafka.PaymentRequested} belongs to the retired direct-publish
 * boundary kept only for reference; collapsing them into one shared type would re-couple two
 * adapters that ADR-0007 says must never reference each other.
 *
 * <p>Whatever is serialized here becomes the Kafka record VALUE verbatim - the Debezium outbox
 * event router (infra/compose/connect/payment-outbox-connector.json) is configured with
 * {@code table.expand.json.payload=true}, which parses this JSON and re-emits its top-level
 * fields as the message value with no wrapping. psp-connector's
 * {@code adapters.in.kafka.PaymentRequestedEvent} must keep deserializing successfully - see
 * services/payment-api/README.md's M6 section for the verified consumed-message comparison.
 *
 * @param paymentId  also the record key (ADR-0003), same as the retired direct-publish path.
 * @param merchantId the owning merchant; not the partition key on this topic (ADR-0003).
 */
public record PaymentRequestedOutboxPayload(
        EventEnvelope envelope,
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status)
        implements DomainEvent {
}
