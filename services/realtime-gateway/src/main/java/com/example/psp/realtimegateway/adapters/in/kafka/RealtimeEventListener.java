package com.example.psp.realtimegateway.adapters.in.kafka;

import com.example.psp.realtimegateway.application.BroadcastRealtimeEventUseCase;
import com.example.psp.realtimegateway.domain.model.RealtimeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * The single consumer for all 7 topics this gateway watches - see
 * {@code config.KafkaConsumerConfig}'s javadoc for the unique-per-instance {@code group.id} this
 * listener's container is built with (the module's central point), and
 * {@code RealtimeEventMapper}'s javadoc for why one listener method can cover 7 distinct Avro
 * schemas.
 *
 * <p>No DLQ, no retry-topic chain (docs/diagrams/topic-map.md: "analytics and realtime-gateway
 * deliberately have none: they log, count, and skip, and are rebuilt by resetting offsets") - a
 * malformed or unrecognized record is logged and the offset is still acknowledged, because this
 * consumer has no durable side effect to protect: a dropped event just never appeared on a
 * browser that happened to be connected at that instant, and every OTHER live connection is
 * unaffected. Contrast with psp-connector's {@code PaymentRequestedListener}, where a skipped
 * record would mean a payment is never authorized - the DLQ-worthiness of a failure is a function
 * of what work is lost, not a blanket policy.
 *
 * <p>{@link #onMessage} hands off to {@link BroadcastRealtimeEventUseCase}, which returns as soon
 * as the registry has DISPATCHED (not delivered) the event to every matching subscription's own
 * executor - see {@code adapters.out.sse.InMemorySseConnectionRegistry}'s javadoc. This method
 * therefore never blocks on browser I/O, and acks immediately after.
 *
 * <p><b>Why the parameter is {@code ConsumerRecord<String, Object>}, not a bare {@code Object}
 * payload:</b> every other listener in this codebase takes a concrete generated Avro type
 * (e.g. {@code PaymentRequested event}), which Spring Kafka's message-conversion layer resolves
 * unambiguously to {@code record.value()}. This listener deliberately takes {@code Object} (see
 * {@code RealtimeEventMapper}'s javadoc for why - one method must accept 7 different concrete
 * types). Verified against this live cluster: a bare {@code Object event} parameter (with or
 * without an explicit {@code @Payload} annotation) is ambiguous enough that Spring Kafka's
 * argument resolution hands back the RAW {@code ConsumerRecord} instead of its converted value -
 * {@code Object} trivially matches {@code ConsumerRecord} too, so the framework never invokes the
 * value converter. Taking {@code ConsumerRecord<String, Object>} explicitly and reading
 * {@code record.value()} ourselves sidesteps that ambiguity entirely: it is a directly-supported,
 * unambiguous Spring Kafka listener parameter type, and {@code value()} already IS the correctly
 * deserialized Avro instance (the {@code ConsumerFactory}'s {@code KafkaAvroDeserializer} with
 * {@code specific.avro.reader=true} did that work before this method ever runs).
 */
@Component
public class RealtimeEventListener {

    private static final Logger log = LoggerFactory.getLogger(RealtimeEventListener.class);

    private final BroadcastRealtimeEventUseCase broadcastUseCase;
    private final RealtimeEventMapper mapper;

    public RealtimeEventListener(BroadcastRealtimeEventUseCase broadcastUseCase, RealtimeEventMapper mapper) {
        this.broadcastUseCase = broadcastUseCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = {
                "${realtime-gateway.kafka.payment-requested-topic}",
                "${realtime-gateway.kafka.payment-status-changed-topic}",
                "${realtime-gateway.kafka.refund-requested-topic}",
                "${realtime-gateway.kafka.funds-reserved-topic}",
                "${realtime-gateway.kafka.refund-completed-topic}",
                "${realtime-gateway.kafka.refund-failed-topic}",
                "${realtime-gateway.kafka.reservation-released-topic}"
            },
            containerFactory = "realtimeKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        Object event = record.value();
        try {
            RealtimeEvent realtimeEvent = mapper.toDomain(event);
            log.debug(
                    "Broadcasting eventType={} paymentId={} merchantId={}",
                    realtimeEvent.eventType(),
                    realtimeEvent.paymentId(),
                    realtimeEvent.merchantId());
            broadcastUseCase.execute(realtimeEvent);
        } catch (RuntimeException ex) {
            // Log, count, skip - see class javadoc. Never left unacknowledged: there is nothing
            // here worth redelivering a record for.
            log.warn("Failed to process realtime event topic={} {}, skipping", record.topic(), event, ex);
        }
        ack.acknowledge();
    }
}
