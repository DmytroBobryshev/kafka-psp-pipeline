package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentExpirationEventPublisher;
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
 * Real Kafka adapter for {@link PaymentExpirationEventPublisher} (M22) - payment-api's first-ever
 * PRODUCER on {@code payments.payment-status-changed.v1} (it has only ever been a consumer of this
 * topic, since M19). Mirrors psp-connector's own {@code KafkaPaymentStatusPublisher} for the
 * identical topic: keyed by {@code merchantId}, not {@code paymentId} - same ADR-0003 reasoning
 * that class's javadoc gives (every status change for one merchant must land on one partition, in
 * order, for the ledger's single-writer-per-balance invariant) - and the same blocking
 * {@code send().get()} + {@link KafkaException} wrapping, rather than
 * {@code KafkaMerchantConfigPublisher}'s {@link IllegalStateException}: this producer shares this
 * topic's existing producer-side convention (psp-connector), not payment-api's own
 * merchants.merchant-config-changed.v1 producer's.
 *
 * <p>Serializer/Schema-Registry wiring mirrors {@code config.MerchantConfigKafkaConfig}'s producer
 * (the other topic this service produces to directly) - see {@code config.PaymentExpirationKafkaConfig}.
 */
@Component
public class KafkaPaymentExpirationPublisher implements PaymentExpirationEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPaymentExpirationPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PaymentExpirationAvroEventFactory avroEventFactory;
    private final String topic;

    public KafkaPaymentExpirationPublisher(
            @Qualifier("paymentExpirationKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
            PaymentExpirationAvroEventFactory avroEventFactory,
            @Value("${payment-api.kafka.payment-status-changed-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroEventFactory = avroEventFactory;
        this.topic = topic;
    }

    @Override
    public void publishExpired(Payment payment, UUID eventId, Instant occurredAt) {
        var event = avroEventFactory.toAvro(eventId, payment, occurredAt);

        // Key = merchantId (ADR-0003), NOT paymentId - see this class's javadoc.
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, payment.getMerchantId(), event);
        record.headers()
                .add("event-id", eventId.toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", "payments.payment-status-changed.v1".getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", payment.getId().toString().getBytes(StandardCharsets.UTF_8));

        try {
            SendResult<String, Object> result = kafkaTemplate.send(record).get();
            RecordMetadata metadata = result.getRecordMetadata();
            log.info(
                    "Published EXPIRED paymentId={} merchantId={} eventId={} -> {}-{}@{}",
                    payment.getId(),
                    payment.getMerchantId(),
                    eventId,
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException(
                    "interrupted while publishing EXPIRED for paymentId=" + payment.getId(), e);
        } catch (ExecutionException e) {
            throw new KafkaException(
                    "failed to publish EXPIRED for paymentId=" + payment.getId(), e.getCause());
        }
    }
}
