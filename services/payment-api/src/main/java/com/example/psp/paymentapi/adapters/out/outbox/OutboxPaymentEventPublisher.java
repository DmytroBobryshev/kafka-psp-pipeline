package com.example.psp.paymentapi.adapters.out.outbox;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.stereotype.Component;

/**
 * Transactional-outbox adapter for {@link PaymentEventPublisher} (M6), replacing
 * {@code adapters.out.kafka.KafkaPaymentEventPublisher} (M3) on the write path. This is the fix
 * for the dual-write problem that class's Javadoc and services/payment-api/README.md document:
 * instead of calling Kafka after the payment row commits - two separate systems, no shared
 * transaction, a crash between them loses the event - this adapter writes a row to the
 * {@code outbox_event} table using plain JPA. Called from
 * {@link com.example.psp.paymentapi.application.CreatePaymentUseCase#execute}, which is
 * {@code @Transactional}: that annotation is what makes this write and
 * {@code PaymentRepository.save} commit atomically, in the SAME Postgres transaction, on the SAME
 * connection. Nothing in this class opens a transaction itself - it just participates in
 * whichever one is already open when it's called.
 *
 * <p>No network call happens here. {@code publishPaymentCreated} returning successfully means
 * only "the outbox row is staged for the current transaction to commit" - actual delivery to
 * {@code payments.payment-requested.v1} happens later, out of process, when Debezium's Kafka
 * Connect connector (infra/compose) reads this table's write-ahead log and the outbox event
 * router SMT republishes each row. See services/payment-api/README.md's M6 section for the full
 * Connect architecture and the delivery-guarantee change that buys.
 *
 * <p>Builds the exact same {@link EventEnvelope} shape {@code KafkaPaymentEventPublisher} used to
 * build (same event type, aggregate type, source, trace/correlation-id-from-MDC fallback), then
 * serializes {@link PaymentRequestedOutboxPayload} with
 * {@link JacksonUtils#enhancedObjectMapper()} - the exact {@link ObjectMapper} Spring Kafka's own
 * {@code JsonSerializer} used for the retired direct-publish path - so the JSON text landing in
 * {@code outbox_event.payload} is byte-identical to what {@code KafkaPaymentEventPublisher} used
 * to put on the wire (including the {@code occurredAt} epoch-decimal quirk documented in the
 * README's "Known issues"). That byte-identity is what lets the Debezium outbox event router
 * re-emit this payload unchanged and keep psp-connector's consumer working with zero changes.
 */
@Component
public class OutboxPaymentEventPublisher implements PaymentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPaymentEventPublisher.class);
    private static final String EVENT_TYPE = "payments.payment-requested.v1";
    private static final String SOURCE = "payment-api";
    private static final String AGGREGATE_TYPE = "payment";

    private final OutboxEventJpaRepository outboxEventJpaRepository;
    private final PaymentOutboxEventMapper eventMapper;
    private final ObjectMapper objectMapper;

    public OutboxPaymentEventPublisher(
            OutboxEventJpaRepository outboxEventJpaRepository, PaymentOutboxEventMapper eventMapper) {
        this.outboxEventJpaRepository = outboxEventJpaRepository;
        this.eventMapper = eventMapper;
        // Same ObjectMapper Spring Kafka's JsonSerializer builds internally - see class Javadoc
        // for why byte-identical serialization here matters.
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
    }

    @Override
    public void publishPaymentCreated(Payment payment) {
        // Same MDC-correlation-id-or-fresh-UUID fallback as the retired KafkaPaymentEventPublisher
        // (real W3C traceparent propagation is M15 scope, not this module's).
        String correlationId = MDC.get("correlationId");
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        String traceId = correlationId;

        String paymentId = payment.getId().toString();
        EventEnvelope envelope =
                EventEnvelope.root(EVENT_TYPE, 1, paymentId, AGGREGATE_TYPE, SOURCE, traceId, correlationId);
        PaymentRequestedOutboxPayload payload = eventMapper.toPayload(envelope, payment);

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            // A domain record failing to serialize is a programming error (missing Jackson
            // module, non-serializable field type), not a runtime/environment failure - same
            // "let it propagate" stance the rest of this service takes on programming errors.
            throw new IllegalStateException("Failed to serialize outbox payload for paymentId=" + paymentId, e);
        }

        OutboxEventEntity entity = new OutboxEventEntity();
        // envelope.eventId() doubles as the outbox row id - the Debezium outbox event router's
        // table.field.event.id maps straight to it, so the row's primary key IS the event's
        // idempotency key (ADR-0002), not a second, independent id.
        entity.setId(envelope.eventId());
        entity.setAggregateType(AGGREGATE_TYPE);
        entity.setAggregateId(paymentId);
        entity.setEventType(EVENT_TYPE);
        entity.setPayload(payloadJson);
        entity.setCreatedAt(envelope.occurredAt());

        outboxEventJpaRepository.save(entity);

        log.info(
                "Staged outbox event outboxId={} eventType={} paymentId={} - relayed to Kafka by"
                        + " Debezium, not this process",
                entity.getId(),
                EVENT_TYPE,
                paymentId);
    }
}
