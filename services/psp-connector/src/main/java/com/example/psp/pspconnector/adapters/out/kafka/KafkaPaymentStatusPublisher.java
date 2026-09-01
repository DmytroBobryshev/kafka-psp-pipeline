package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.common.events.UuidV7;
import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import com.example.psp.pspconnector.domain.port.PaymentStatusPublisher;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
public class KafkaPaymentStatusPublisher implements PaymentStatusPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPaymentStatusPublisher.class);
    private static final String EVENT_TYPE = "payments.payment-status-changed.v1";
    private static final String SOURCE = "psp-connector";
    private static final String AGGREGATE_TYPE = "payment";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PaymentStatusAvroEventFactory avroEventFactory;
    private final String topic;

    public KafkaPaymentStatusPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            PaymentStatusAvroEventFactory avroEventFactory,
            @Value("${psp-connector.kafka.payment-status-changed-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroEventFactory = avroEventFactory;
        this.topic = topic;
    }

    @Override
    public void publishPending(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID causationEventId,
            String traceId,
            String correlationId) {
        publishNonTerminal(
                "PENDING", paymentId, merchantId, amount, "", causationEventId, traceId, correlationId);
    }

    @Override
    public void publishIpnReceived(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId) {
        publishNonTerminal(
                "IPN_RECEIVED",
                paymentId,
                merchantId,
                amount,
                providerReference.toString(),
                causationEventId,
                traceId,
                correlationId);
    }

    @Override
    public void publishVerified(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId) {
        publishNonTerminal(
                "VERIFIED",
                paymentId,
                merchantId,
                amount,
                providerReference.toString(),
                causationEventId,
                traceId,
                correlationId);
    }

    private void publishNonTerminal(
            String status,
            UUID paymentId,
            String merchantId,
            Money amount,
            String providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId) {
        EventEnvelope envelope =
                EventEnvelope.causedBy(
                        causationEventId, EVENT_TYPE, 1, paymentId.toString(), AGGREGATE_TYPE, SOURCE,
                        traceId, correlationId);
        var event =
                avroEventFactory.toNonTerminalAvro(
                        envelope, paymentId, merchantId, amount, status, providerReference);
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, merchantId, event);
        record.headers()
                .add("event-id", envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", envelope.eventType().getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", envelope.aggregateId().getBytes(StandardCharsets.UTF_8));
        try {
            kafkaTemplate.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("interrupted while publishing " + status + " for paymentId=" + paymentId, e);
        } catch (ExecutionException e) {
            throw new KafkaException("failed to publish " + status + " for paymentId=" + paymentId, e.getCause());
        }
    }

    @Override
    public void publishStatusChanged(PaymentAttempt attempt) {
        if (attempt.getOutcome() == ProviderOutcome.TIMEOUT) {
            throw new IllegalStateException(
                    "must never publish payments.payment-status-changed.v1 for a TIMEOUT outcome (ADR-0006), paymentId="
                            + attempt.getPaymentId());
        }

        UUID eventId = attempt.getStatusEventId() != null ? attempt.getStatusEventId() : UuidV7.generate();
        EventEnvelope envelope =
                EventEnvelope.causedBy(
                        eventId,
                        attempt.getCausationEventId(),
                        EVENT_TYPE,
                        1,
                        attempt.getPaymentId().toString(),
                        AGGREGATE_TYPE,
                        SOURCE,
                        attempt.getTraceId(),
                        attempt.getCorrelationId());
        com.example.psp.common.events.avro.PaymentStatusChanged event = avroEventFactory.toAvro(envelope, attempt);

        // Key = merchantId, NOT paymentId - see this class's javadoc.
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, attempt.getMerchantId(), event);
        record.headers()
                .add("event-id", envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", envelope.eventType().getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", envelope.aggregateId().getBytes(StandardCharsets.UTF_8));

        try {
            SendResult<String, Object> result = kafkaTemplate.send(record).get();
            RecordMetadata metadata = result.getRecordMetadata();
            log.info(
                    "Published {} paymentId={} merchantId={} status={} eventId={} partition={} offset={}",
                    topic,
                    attempt.getPaymentId(),
                    attempt.getMerchantId(),
                    event.getStatus(),
                    eventId,
                    metadata.partition(),
                    metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException(
                    "interrupted while publishing " + topic + " for paymentId=" + attempt.getPaymentId(), e);
        } catch (ExecutionException e) {
            throw new KafkaException(
                    "failed to publish " + topic + " for paymentId=" + attempt.getPaymentId(), e.getCause());
        }
    }
}
