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

/**
 * Direct-to-Kafka adapter for {@link MerchantConfigPublisher} (M10) - the one write path in this
 * service that does <b>not</b> go through the M6 outbox. The justification lives on the port
 * interface (short version: there is no second write to be atomic with, the outbox column is
 * {@code NOT NULL} so it cannot carry a tombstone, and the single Debezium connector is
 * hard-wired to {@code payments.payment-requested.v1}).
 *
 * <h2>The two record shapes</h2>
 *
 * <pre>
 *   PUT    -> ProducerRecord(topic, key=merchantId, value=MerchantConfigChanged)  // Avro bytes
 *   DELETE -> ProducerRecord(topic, key=merchantId, value=null)                   // TOMBSTONE
 * </pre>
 *
 * <p>The tombstone's {@code null} is load-bearing in three separate places at once, which is why
 * a {@code deleted=true} flag is not an equivalent design:
 *
 * <ol>
 *   <li><b>The broker's log cleaner</b> treats a null value as "this key has no value", so after
 *       compaction the key is absent from the log entirely. A flag is a value, so compaction
 *       retains it forever and the log never shrinks by one byte.</li>
 *   <li><b>Kafka Streams</b> ({@code KTable} / {@code GlobalKTable}) deletes the row when it sees
 *       a null value - a lookup afterwards returns {@code null} with no consumer-side code. With
 *       a flag, every consumer in the fleet must remember to check it, which is one new consumer
 *       away from being forgotten, and the failure is silent (a suspended merchant looks
 *       active).</li>
 *   <li><b>Consumers generally</b> - Connect sinks, ksqlDB, the Mongo sink in M13 - already agree
 *       that null means delete. It is the ecosystem's only portable delete signal.</li>
 * </ol>
 *
 * <h2>Why the ADR-0002 envelope headers matter here specifically</h2>
 *
 * <p>ADR-0002 duplicates {@code event-id} / {@code event-type} / {@code aggregate-id} into Kafka
 * headers "for infrastructure that must route, filter or trace without deserializing". A
 * tombstone is the case where that duplication stops being a convenience: it has no value, so
 * there is nowhere else for provenance to live. Both record shapes therefore carry the headers,
 * and for a tombstone they are the only trace of who deleted what and when.
 *
 * <h2>Synchronous send, on purpose</h2>
 *
 * <p>{@code KafkaTemplate#send} is async; this adapter blocks on the result. A config change the
 * operator believes succeeded but which never reached the topic is worse than an honest 503,
 * and - unlike the payment path - there is no outbox row standing behind the call to make it
 * eventually true. The block is what turns a broker outage into a failed {@code PUT}.
 */
@Component
public class KafkaMerchantConfigPublisher implements MerchantConfigPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaMerchantConfigPublisher.class);

    private static final String EVENT_TYPE = "merchants.merchant-config-changed.v1";
    private static final String SOURCE = "payment-api";
    private static final String AGGREGATE_TYPE = "merchant";

    /** ADR-0002 header names - written by this adapter, read by AKHQ/DLQ triage/M15 tracing. */
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

        // Key = merchantId (ADR-0003). On a compacted topic the key is the identity compaction
        // dedupes on, not just a partition hint - get it wrong and compaction merges or splits
        // the wrong entities.
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

        // THE tombstone. `null` value - not an empty Avro record, not a flag. The value
        // serializer is never invoked for a null value (KafkaAvroSerializer#serialize returns
        // null immediately), so this record carries zero bytes of value and no schema id, and is
        // never validated against merchants.merchant-config-changed.v1-value.
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
            // Surfaces as a 500 via common-web's GlobalExceptionHandler. Safe for the caller to
            // retry blindly: both operations are whole-state writes under a fixed key, so a
            // duplicate converges on the same compacted last-value.
            throw new IllegalStateException("Failed to publish to " + topic + " within " + SEND_TIMEOUT, e);
        }
    }

    /**
     * Same MDC-correlation-id-or-fresh-UUID fallback as the payment path (real W3C traceparent
     * propagation is M15 scope).
     */
    private EventEnvelope newEnvelope(String merchantId) {
        String correlationId = MDC.get("correlationId");
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        return EventEnvelope.root(EVENT_TYPE, 1, merchantId, AGGREGATE_TYPE, SOURCE, correlationId, correlationId);
    }
}
