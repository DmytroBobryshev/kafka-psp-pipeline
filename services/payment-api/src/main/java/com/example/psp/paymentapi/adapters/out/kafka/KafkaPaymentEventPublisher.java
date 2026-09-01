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
        record.headers()
                .add("traceparent", traceId.getBytes(StandardCharsets.UTF_8))
                .add("event-id", envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", envelope.eventType().getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", envelope.aggregateId().getBytes(StandardCharsets.UTF_8));

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
