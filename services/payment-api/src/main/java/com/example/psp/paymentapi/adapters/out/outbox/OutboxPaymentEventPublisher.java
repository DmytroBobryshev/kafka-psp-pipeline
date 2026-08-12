package com.example.psp.paymentapi.adapters.out.outbox;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
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
 *
 * <h2>M15 - bridging the outbox hop</h2>
 *
 * <p>The M6 outbox breaks the in-process span chain by construction: this method commits a row
 * and returns; the record reaches Kafka LATER, from Debezium/Kafka Connect - a different process
 * that never had this request's span on its call stack. Left alone, psp-connector's consumer would
 * find no {@code traceparent} header and start a brand-new trace, disconnected from payment-api's.
 * The fix: read the CURRENT span here (via {@link Tracer}, not by hand-parsing anything) and
 * {@link Propagator#inject} it into {@link OutboxEventEntity#getTraceParent()}. Debezium's outbox
 * event router is configured ({@code infra/compose/connect/payment-outbox-connector.json}'s
 * {@code table.fields.additional.placement}) to copy that column onto the relayed record as the
 * {@code traceparent} header - the same header Micrometer's Kafka observation instrumentation
 * would have written had this producer called Kafka directly. See
 * {@code db/migration/V5__outbox_event_trace_parent.sql} for the column and
 * services/payment-api/README.md's M15 section for the full propagation write-up, including the
 * honest limit (the relay is a header copy, not a new span - there is no span representing
 * "Debezium relayed this row").
 *
 * <p>{@link EventEnvelope#traceId()} is set from the SAME current span, not from
 * {@code correlationId} - this is ADR-0002's original intent for the field ("W3C trace-id,
 * propagated end to end (M15)"), finished here: every downstream {@code causedBy} envelope forwards
 * whatever {@code traceId} it received (see psp-connector's and ledger's publishers), so setting it
 * correctly once, at the root, is what makes the value in the Avro payload and the value in the
 * {@code traceparent} header agree everywhere downstream - one notion of trace identity, not two.
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
    private final Tracer tracer;
    private final Propagator propagator;

    public OutboxPaymentEventPublisher(
            OutboxEventJpaRepository outboxEventJpaRepository,
            PaymentAvroEventFactory avroEventFactory,
            KafkaAvroSerializer paymentRequestedAvroSerializer,
            Tracer tracer,
            Propagator propagator) {
        this.outboxEventJpaRepository = outboxEventJpaRepository;
        this.avroEventFactory = avroEventFactory;
        this.avroSerializer = paymentRequestedAvroSerializer;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    @Override
    public void publishPaymentCreated(Payment payment) {
        // correlationId stays MDC-or-fresh-UUID (CorrelationIdFilter, libs/common-web) - the
        // originating REQUEST id, stable across retries of the same logical request even if a
        // retry gets its own trace. traceId below is now the real thing - see class javadoc.
        String correlationId = MDC.get("correlationId");
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        Span currentSpan = tracer.currentSpan();
        String traceId = currentSpan != null ? currentSpan.context().traceId() : correlationId;

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
        entity.setTraceParent(traceParentOf(currentSpan));

        outboxEventJpaRepository.save(entity);

        log.info(
                "Staged outbox event outboxId={} eventType={} paymentId={} payloadBytes={} traceId={} - relayed"
                        + " to Kafka by Debezium, not this process",
                entity.getId(),
                EVENT_TYPE,
                paymentId,
                wireBytes.length,
                traceId);
    }

    /**
     * Renders {@code currentSpan}'s context as a real W3C {@code traceparent} string via the
     * OTel-bridged {@link Propagator} - never hand-formatted (see class javadoc). Returns
     * {@code null} when there is no active span, which {@link OutboxEventEntity#traceParent} is
     * built to tolerate (see that field's javadoc).
     */
    private String traceParentOf(Span currentSpan) {
        if (currentSpan == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(currentSpan.context(), carrier, Map::put);
        return carrier.get("traceparent");
    }
}
