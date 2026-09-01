package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.DocumentReference;
import com.example.psp.paymentapi.domain.port.DisputeEventPublisher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
public class KafkaDisputeEventPublisher implements DisputeEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaDisputeEventPublisher.class);

    private static final String EVENT_TYPE = "disputes.dispute-opened.v1";
    private static final String SOURCE = "payment-api";
    private static final String AGGREGATE_TYPE = "dispute";

    private static final String HEADER_EVENT_ID = "event-id";
    private static final String HEADER_EVENT_TYPE = "event-type";
    private static final String HEADER_AGGREGATE_ID = "aggregate-id";

    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final DisputeAvroEventFactory avroEventFactory;
    private final String topic;

    public KafkaDisputeEventPublisher(
            @Qualifier("disputeKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
            DisputeAvroEventFactory avroEventFactory,
            @Value("${payment-api.kafka.dispute-opened-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroEventFactory = avroEventFactory;
        this.topic = topic;
    }

    @Override
    public void publishInline(
            UUID disputeId, UUID paymentId, String merchantId, String reason, byte[] documentBytes,
            String contentType) {
        EventEnvelope envelope = newEnvelope(disputeId);
        com.example.psp.common.events.avro.DisputeOpened event =
                avroEventFactory.toInlineAvro(
                        envelope,
                        disputeId.toString(),
                        paymentId.toString(),
                        merchantId,
                        reason,
                        documentBytes,
                        contentType);

        RecordMetadata metadata = send(disputeId, event, envelope);
        log.info(
                "Published dispute-opened (INLINE, {} bytes) disputeId={} paymentId={} eventId={} -> {}-{}@{}",
                documentBytes.length,
                disputeId,
                paymentId,
                envelope.eventId(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset());
    }

    @Override
    public void publishClaimChecked(
            UUID disputeId, UUID paymentId, String merchantId, String reason, DocumentReference reference) {
        EventEnvelope envelope = newEnvelope(disputeId);
        com.example.psp.common.events.avro.DisputeOpened event =
                avroEventFactory.toClaimCheckAvro(
                        envelope, disputeId.toString(), paymentId.toString(), merchantId, reason, reference);

        RecordMetadata metadata = send(disputeId, event, envelope);
        log.info(
                "Published dispute-opened (CLAIM CHECK, {} bytes at {}/{}) disputeId={} paymentId={}"
                        + " eventId={} -> {}-{}@{}",
                reference.sizeBytes(),
                reference.bucket(),
                reference.objectKey(),
                disputeId,
                paymentId,
                envelope.eventId(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset());
    }

    private RecordMetadata send(
            UUID disputeId, com.example.psp.common.events.avro.DisputeOpened event, EventEnvelope envelope) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, disputeId.toString(), event);
        record.headers()
                .add(HEADER_EVENT_ID, envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add(HEADER_EVENT_TYPE, EVENT_TYPE.getBytes(StandardCharsets.UTF_8))
                .add(HEADER_AGGREGATE_ID, disputeId.toString().getBytes(StandardCharsets.UTF_8));
        try {
            SendResult<String, Object> result =
                    kafkaTemplate.send(record).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return result.getRecordMetadata();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing to " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to publish to " + topic + " within " + SEND_TIMEOUT, e);
        }
    }

    private EventEnvelope newEnvelope(UUID disputeId) {
        String correlationId = MDC.get("correlationId");
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        return EventEnvelope.root(
                EVENT_TYPE, 1, disputeId.toString(), AGGREGATE_TYPE, SOURCE, correlationId, correlationId);
    }
}
