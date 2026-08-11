package com.example.psp.paymentapi.adapters.out.outbox;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
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
 * <p>No Kafka network call happens here, same as through M6. {@code publishPaymentCreated}
 * returning successfully means only "the outbox row is staged for the current transaction to
 * commit" - actual delivery to {@code payments.payment-requested.v1} happens later, out of
 * process, when Debezium's Kafka Connect connector (infra/compose) reads this table's
 * write-ahead log and the outbox event router SMT republishes each row. See
 * services/payment-api/README.md's M6 section for the full Connect architecture.
 *
 * <h2>M9 Phase 1 - the outbox-serialization decision</h2>
 *
 * <p>{@code payments.payment-requested.v1} is now Avro + Schema Registry (the only topic migrated
 * so far - every other topic stays JSON). Since payment-api never calls Kafka, something still
 * has to produce the exact Confluent wire format (1 magic byte + 4-byte schema id + Avro binary)
 * that a standard Avro consumer expects. This class does it here, directly: it builds the same
 * {@link EventEnvelope} {@code KafkaPaymentEventPublisher} used to build (same event type,
 * aggregate type, source, trace/correlation-id-from-MDC fallback), converts it to the generated
 * Avro record via {@link PaymentAvroEventFactory}, and calls {@link KafkaAvroSerializer#serialize}
 * - which registers/looks up the schema against Schema Registry and returns the finished wire
 * bytes - BEFORE ever touching Postgres. Those exact bytes are what gets written to
 * {@code outbox_event.payload} (now {@code BYTEA}, not {@code JSONB} - see
 * {@code db/migration/V3__outbox_event_payload_bytes.sql}), and
 * {@code infra/compose/connect/payment-outbox-connector.json} passes them through unchanged with
 * {@code ByteArrayConverter}. See the README's M9 section for the alternatives this rejected
 * (Connect-side {@code AvroConverter} inferring a schema from the JSON payload) and why.
 */
@Component
public class OutboxPaymentEventPublisher implements PaymentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPaymentEventPublisher.class);
    private static final String EVENT_TYPE = "payments.payment-requested.v1";
    private static final String SOURCE = "payment-api";
    private static final String AGGREGATE_TYPE = "payment";

    private final OutboxEventJpaRepository outboxEventJpaRepository;
    private final PaymentAvroEventFactory avroEventFactory;
    private final KafkaAvroSerializer avroSerializer;

    public OutboxPaymentEventPublisher(
            OutboxEventJpaRepository outboxEventJpaRepository,
            PaymentAvroEventFactory avroEventFactory,
            KafkaAvroSerializer paymentRequestedAvroSerializer) {
        this.outboxEventJpaRepository = outboxEventJpaRepository;
        this.avroEventFactory = avroEventFactory;
        this.avroSerializer = paymentRequestedAvroSerializer;
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

        com.example.psp.common.events.avro.PaymentRequested avroEvent = avroEventFactory.toAvro(envelope, payment);

        // The topic name passed here is what KafkaAvroSerializer combines with TopicNameStrategy
        // to derive the Schema Registry subject: payments.payment-requested.v1-value (ADR-0001).
        // Returns the complete Confluent wire format - see the class javadoc.
        byte[] wireBytes = avroSerializer.serialize(EVENT_TYPE, avroEvent);

        OutboxEventEntity entity = new OutboxEventEntity();
        // envelope.eventId() doubles as the outbox row id - the Debezium outbox event router's
        // table.field.event.id maps straight to it, so the row's primary key IS the event's
        // idempotency key (ADR-0002), not a second, independent id. This is unchanged by M9: it
        // is the hand-written envelope's id, not anything inside the Avro wire bytes.
        entity.setId(envelope.eventId());
        entity.setAggregateType(AGGREGATE_TYPE);
        entity.setAggregateId(paymentId);
        entity.setEventType(EVENT_TYPE);
        entity.setPayload(wireBytes);
        entity.setCreatedAt(envelope.occurredAt());

        outboxEventJpaRepository.save(entity);

        log.info(
                "Staged outbox event outboxId={} eventType={} paymentId={} payloadBytes={} - relayed"
                        + " to Kafka by Debezium, not this process",
                entity.getId(),
                EVENT_TYPE,
                paymentId,
                wireBytes.length);
    }
}
