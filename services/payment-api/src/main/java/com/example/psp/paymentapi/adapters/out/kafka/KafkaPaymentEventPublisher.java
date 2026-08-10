package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentEventPublisher;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Real Kafka adapter for {@link PaymentEventPublisher} (M3), replacing the M1
 * {@code LoggingPaymentEventPublisher} stub (deleted). Publishes
 * {@code payments.payment-requested.v1}, keyed by {@code paymentId} (ADR-0003: this topic emits
 * exactly one event per payment, so the key buys even spread across psp-connector's 12 partitions
 * rather than any ordering guarantee - there is nothing to order on a single-event-per-aggregate
 * topic).
 *
 * <p>Producer settings (acks, retries, idempotence, batching, compression) live in
 * {@code application.yml}'s {@code spring.kafka.producer} block and are wired into the
 * {@link KafkaTemplate} bean in {@code config.KafkaProducerConfig}; this class only builds the
 * record and headers.
 *
 * <p><b>Retired from the write path by M6.</b> Through M3-M5 this was called from
 * {@link com.example.psp.paymentapi.application.CreatePaymentUseCase} AFTER the Postgres row was
 * already committed - two separate systems, no shared transaction, so a crash between those two
 * calls lost the event with no retry (the "dual-write problem" - see
 * services/payment-api/README.md's M3 and M6 sections). M6 replaces this class on the POST path
 * with {@code adapters.out.outbox.OutboxPaymentEventPublisher}, which writes an outbox row in the
 * SAME transaction as the payment instead of calling Kafka directly; Debezium relays that row to
 * this exact topic asynchronously (infra/compose). The {@code @Component} annotation is removed
 * deliberately so Spring never wires this class in - it is kept only as a reference for what
 * direct (non-transactional) publishing used to look like, and because
 * {@code adapters.out.outbox.OutboxPaymentEventPublisher} intentionally mirrors its envelope-
 * building logic. Do not re-annotate this class without also removing the outbox adapter, or
 * {@link PaymentEventPublisher} will have two competing bean definitions.
 */
public class KafkaPaymentEventPublisher implements PaymentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPaymentEventPublisher.class);
    private static final String EVENT_TYPE = "payments.payment-requested.v1";
    private static final String SOURCE = "payment-api";
    private static final String AGGREGATE_TYPE = "payment";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PaymentEventMapper eventMapper;
    private final String topic;

    public KafkaPaymentEventPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            PaymentEventMapper eventMapper,
            @Value("${payment-api.kafka.payment-requested-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventMapper = eventMapper;
        this.topic = topic;
    }

    @Override
    public void publishPaymentCreated(Payment payment) {
        // Real W3C trace-context propagation (a `traceparent` header read into
        // EventEnvelope.traceId) lands in M15 with Micrometer Tracing/OTel. Until then, reuse
        // common-web's per-request correlation id (already in the MDC via CorrelationIdFilter)
        // as both traceId and correlationId so the envelope's non-blank invariant holds and log
        // correlation still works end to end. Falls back to a fresh id when publishing happens
        // outside a request thread (e.g. the M3 throughput/data-loss harnesses).
        String correlationId = MDC.get("correlationId");
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        String traceId = correlationId;

        String paymentId = payment.getId().toString();
        EventEnvelope envelope =
                EventEnvelope.root(EVENT_TYPE, 1, paymentId, AGGREGATE_TYPE, SOURCE, traceId, correlationId);
        PaymentRequested event = eventMapper.toEvent(envelope, payment);

        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, paymentId, event);
        // Header duplication (ADR-0002): infrastructure that must route/filter/trace without
        // deserializing the value (DLQ triage, AKHQ filtering, future OTel propagation) reads
        // these; the value stays the single source of truth and wins on any disagreement.
        record.headers()
                .add("traceparent", traceId.getBytes(StandardCharsets.UTF_8))
                .add("event-id", envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", envelope.eventType().getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", envelope.aggregateId().getBytes(StandardCharsets.UTF_8));

        // Async send: never block the HTTP request thread on a broker round trip. The callback
        // logs partition+offset on success, or the exception on failure - see
        // services/payment-api/README.md for a captured example of this log line next to the
        // console-consumer output for the same record.
        kafkaTemplate
                .send(record)
                .whenComplete(
                        (result, ex) -> {
                            if (ex != null) {
                                log.error("Failed to publish {} for paymentId={}", topic, paymentId, ex);
                            } else {
                                RecordMetadata metadata = result.getRecordMetadata();
                                log.info(
                                        "Published {} paymentId={} partition={} offset={}",
                                        topic,
                                        paymentId,
                                        metadata.partition(),
                                        metadata.offset());
                            }
                        });
    }
}
