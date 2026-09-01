package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.MerchantConfig;
import com.example.psp.paymentapi.domain.port.MerchantConfigPublisher;
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
public class KafkaMerchantConfigPublisher implements MerchantConfigPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaMerchantConfigPublisher.class);

    private static final String EVENT_TYPE = "merchants.merchant-config-changed.v1";
    private static final String SOURCE = "payment-api";
    private static final String AGGREGATE_TYPE = "merchant";

    private static final String HEADER_EVENT_ID = "event-id";

    private static final String HEADER_EVENT_TYPE = "event-type";
    private static final String HEADER_AGGREGATE_ID = "aggregate-id";

    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MerchantConfigAvroEventFactory avroEventFactory;
    private final String topic;

    public KafkaMerchantConfigPublisher(
            @Qualifier("merchantConfigKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
            MerchantConfigAvroEventFactory avroEventFactory,
            @Value("${payment-api.kafka.merchant-config-changed-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroEventFactory = avroEventFactory;
        this.topic = topic;
    }

    @Override
    public void publishConfigChanged(MerchantConfig config) {
        EventEnvelope envelope = newEnvelope(config.merchantId());
        com.example.psp.common.events.avro.MerchantConfigChanged avroEvent =
                avroEventFactory.toAvro(envelope, config);

        RecordMetadata metadata = send(record(config.merchantId(), avroEvent, envelope));

        log.info(
                "Published merchant config merchantId={} status={} eventId={} -> {}-{}@{}",
                config.merchantId(),
                config.status(),
                envelope.eventId(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset());
    }

    @Override
    public void publishConfigDeleted(String merchantId) {
        EventEnvelope envelope = newEnvelope(merchantId);

        RecordMetadata metadata = send(record(merchantId, null, envelope));

        log.info(
                "Published TOMBSTONE (null value) for merchantId={} eventId={} -> {}-{}@{} - the key"
                        + " is removed from the log at the next compaction pass, and from every"
                        + " downstream GlobalKTable as soon as they consume it",
                merchantId,
                envelope.eventId(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset());
    }

    private ProducerRecord<String, Object> record(String merchantId, Object value, EventEnvelope envelope) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, merchantId, value);
        record.headers()
                .add(HEADER_EVENT_ID, envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add(HEADER_EVENT_TYPE, EVENT_TYPE.getBytes(StandardCharsets.UTF_8))
                .add(HEADER_AGGREGATE_ID, merchantId.getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private RecordMetadata send(ProducerRecord<String, Object> record) {
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

    private EventEnvelope newEnvelope(String merchantId) {
        String correlationId = MDC.get("correlationId");
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        return EventEnvelope.root(EVENT_TYPE, 1, merchantId, AGGREGATE_TYPE, SOURCE, correlationId, correlationId);
    }
}
