package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.paymentapi.domain.model.Refund;
import com.example.psp.paymentapi.domain.port.RefundExpirationEventPublisher;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Real Kafka adapter for {@link RefundExpirationEventPublisher} (M24) - payment-api's first-ever
 * PRODUCER on {@code refunds.refund-status-changed.v1} (it has only ever been a consumer of this
 * topic, since M23). Mirrors {@code KafkaPaymentExpirationPublisher} for the identical shape on the
 * payment side: keyed by {@code merchantId} (ADR-0003 - every refund-saga event for one merchant
 * must land on one partition, in order), blocking {@code send().get()} + {@link KafkaException}
 * wrapping, same producer-side convention this topic's other producer (psp-connector) already
 * follows.
 *
 * <p>Serializer/Schema-Registry wiring mirrors {@code config.PaymentExpirationKafkaConfig}'s
 * producer - see {@code config.RefundExpirationKafkaConfig}.
 */
@Component
public class KafkaRefundExpirationPublisher implements RefundExpirationEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaRefundExpirationPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RefundExpirationAvroEventFactory avroEventFactory;
    private final String topic;

    public KafkaRefundExpirationPublisher(
            @Qualifier("refundExpirationKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
            RefundExpirationAvroEventFactory avroEventFactory,
            @Value("${payment-api.kafka.refund-status-changed-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroEventFactory = avroEventFactory;
        this.topic = topic;
    }

    @Override
    public void publishExpired(Refund refund, UUID eventId, Instant occurredAt) {
        var event = avroEventFactory.toAvro(eventId, refund, occurredAt);

        // Key = merchantId (ADR-0003), NOT refundId - see this class's javadoc.
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, refund.getMerchantId(), event);
        record.headers()
                .add("event-id", eventId.toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", "refunds.refund-status-changed.v1".getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", refund.getId().toString().getBytes(StandardCharsets.UTF_8));

        try {
            SendResult<String, Object> result = kafkaTemplate.send(record).get();
            RecordMetadata metadata = result.getRecordMetadata();
            log.info(
                    "Published EXPIRED refundId={} merchantId={} eventId={} -> {}-{}@{}",
                    refund.getId(),
                    refund.getMerchantId(),
                    eventId,
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException(
                    "interrupted while publishing EXPIRED for refundId=" + refund.getId(), e);
        } catch (ExecutionException e) {
            throw new KafkaException(
                    "failed to publish EXPIRED for refundId=" + refund.getId(), e.getCause());
        }
    }
}
