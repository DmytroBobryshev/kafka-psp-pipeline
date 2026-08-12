package com.example.psp.paymentapi.adapters.out.outbox;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Refund;
import com.example.psp.paymentapi.domain.port.RefundEventPublisher;
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
 * Transactional-outbox adapter for {@link RefundEventPublisher} (M11) - the refund-path
 * counterpart of {@link OutboxPaymentEventPublisher}, same mechanics exactly: no Kafka network
 * call here, only a JPA insert into the SAME {@code outbox_event} table, on the SAME connection as
 * whatever transaction is already open when this is called ({@code application.RequestRefundUseCase},
 * {@code @Transactional}). Debezium relays the row to {@code refunds.refund-requested.v1} later,
 * out of process - unchanged infrastructure, a second aggregate type flowing through the one
 * existing connector (see {@code infra/compose/connect/payment-outbox-connector.json}; both
 * {@code payments} and {@code refunds} route through {@code table.expand.json.payload}-equivalent
 * Avro passthrough, keyed on {@code aggregate_type} the same way M6 already established for
 * payments).
 *
 * <h2>Why {@code outbox_event.aggregate_id} holds {@code merchantId}, not {@code refundId}, here</h2>
 *
 * <p>Debezium's outbox {@code EventRouter} SMT maps the {@code aggregate_id} column directly onto
 * the produced Kafka record's KEY ({@code transforms.outbox.table.field.event.key=aggregate_id},
 * {@code infra/compose/connect/payment-outbox-connector.json}) - one column, shared by every
 * aggregate type that flows through this table. For {@code payments.payment-requested.v1},
 * ADR-0003 keys by {@code paymentId}, which is also that event's natural "entity the event is
 * about" - so {@code OutboxPaymentEventPublisher} can use one value for both the column and the
 * envelope's {@code aggregateId}. ADR-0003 keys EVERY {@code refunds.*.v1} topic by
 * {@code merchantId} instead (so the whole saga serialises against one merchant's ledger balance),
 * while the event's {@code aggregateId} - the entity the event is about, ADR-0002 - is correctly
 * {@code refundId} (matching every other refund-saga event this saga produces). This adapter
 * therefore sets the OUTBOX ROW's {@code aggregate_id} to {@code merchantId} (so Debezium keys the
 * Kafka record correctly) while the EMBEDDED {@link EventEnvelope#aggregateId()} stays
 * {@code refundId} (so the payload's own identity is correct) - two different values serving two
 * different jobs, deliberately not the same field twice.
 *
 * <p>{@code refunds.refund-requested.v1} is Avro from the start (no JSON history to migrate,
 * unlike {@code payments.payment-requested.v1}'s M9 Phase 1 cutover) - this class builds the exact
 * Confluent wire format the same way {@code OutboxPaymentEventPublisher} does: {@link
 * RefundAvroEventFactory} builds the generated Avro record, {@link KafkaAvroSerializer#serialize}
 * registers/looks up the schema and returns the finished wire bytes, written verbatim to
 * {@code outbox_event.payload} ({@code BYTEA}).
 *
 * <h2>M15 - bridging the outbox hop</h2>
 *
 * <p>Same bridge as {@link OutboxPaymentEventPublisher} - see that class's javadoc for the full
 * write-up. {@link EventEnvelope#traceId()} is set from the current span (via {@link Tracer}), and
 * that same span's context is injected (via {@link Propagator}, never hand-formatted) into
 * {@link OutboxEventEntity#getTraceParent()}, which Debezium's outbox event router copies onto the
 * relayed record as the {@code traceparent} header.
 */
@Component
public class OutboxRefundEventPublisher implements RefundEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxRefundEventPublisher.class);
    private static final String EVENT_TYPE = "refunds.refund-requested.v1";
    private static final String SOURCE = "payment-api";
    private static final String AGGREGATE_TYPE = "refund";

    private final OutboxEventJpaRepository outboxEventJpaRepository;
    private final RefundAvroEventFactory avroEventFactory;
    private final KafkaAvroSerializer avroSerializer;
    private final Tracer tracer;
    private final Propagator propagator;

    public OutboxRefundEventPublisher(
            OutboxEventJpaRepository outboxEventJpaRepository,
            RefundAvroEventFactory avroEventFactory,
            KafkaAvroSerializer paymentRequestedAvroSerializer,
            Tracer tracer,
            Propagator propagator) {
        this.outboxEventJpaRepository = outboxEventJpaRepository;
        this.avroEventFactory = avroEventFactory;
        // Reuses the SAME KafkaAvroSerializer bean payment-requested's outbox path uses - it is a
        // stateless encoder configured once with the Schema Registry URL; the topic name passed to
        // serialize(...) is what determines the subject (TopicNameStrategy, ADR-0001), so one
        // instance safely serves every Avro-outbound topic in this service.
        this.avroSerializer = paymentRequestedAvroSerializer;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    @Override
    public void publishRefundRequested(Refund refund) {
        String correlationId = MDC.get("correlationId");
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        Span currentSpan = tracer.currentSpan();
        String traceId = currentSpan != null ? currentSpan.context().traceId() : correlationId;

        String refundId = refund.getId().toString();
        EventEnvelope envelope =
                EventEnvelope.root(EVENT_TYPE, 1, refundId, AGGREGATE_TYPE, SOURCE, traceId, correlationId);

        com.example.psp.common.events.avro.RefundRequested avroEvent = avroEventFactory.toAvro(envelope, refund);

        byte[] wireBytes = avroSerializer.serialize(EVENT_TYPE, avroEvent);

        OutboxEventEntity entity = new OutboxEventEntity();
        entity.setId(envelope.eventId());
        entity.setAggregateType(AGGREGATE_TYPE);
        // merchantId, NOT refundId - see this class's javadoc ("Why outbox_event.aggregate_id
        // holds merchantId"). This is what Debezium's EventRouter uses as the Kafka record KEY
        // (ADR-0003: refunds.*.v1 is keyed by merchantId); the envelope's own aggregateId above
        // stays refundId.
        entity.setAggregateId(refund.getMerchantId());
        entity.setEventType(EVENT_TYPE);
        entity.setPayload(wireBytes);
        entity.setCreatedAt(envelope.occurredAt());
        entity.setTraceParent(traceParentOf(currentSpan));

        outboxEventJpaRepository.save(entity);

        log.info(
                "Staged outbox event outboxId={} eventType={} refundId={} paymentId={} payloadBytes={} "
                        + "traceId={} - relayed to Kafka by Debezium, not this process",
                entity.getId(),
                EVENT_TYPE,
                refundId,
                refund.getPaymentId(),
                wireBytes.length,
                traceId);
    }

    /** See {@link OutboxPaymentEventPublisher#traceParentOf} - identical logic, duplicated rather
     * than shared because these two adapters share no common base class (ADR-0007: each is a
     * standalone hexagon-boundary adapter). */
    private String traceParentOf(Span currentSpan) {
        if (currentSpan == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(currentSpan.context(), carrier, Map::put);
        return carrier.get("traceparent");
    }
}
