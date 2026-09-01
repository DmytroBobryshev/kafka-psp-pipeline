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

        byte[] wireBytes = avroSerializer.serialize(EVENT_TYPE, avroEvent);

        OutboxEventEntity entity = new OutboxEventEntity();
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

    private String traceParentOf(Span currentSpan) {
        if (currentSpan == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(currentSpan.context(), carrier, Map::put);
        return carrier.get("traceparent");
    }
}
