package com.example.psp.ledger.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.ledger.domain.model.MerchantBalance;
import com.example.psp.ledger.domain.model.RefundRequest;
import com.example.psp.ledger.domain.model.RefundSagaState;
import com.example.psp.ledger.domain.port.RefundEventPublisher;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Real Kafka adapter for {@link RefundEventPublisher} (M11). Uses the SAME transactional
 * {@link KafkaTemplate} M7's {@code KafkaLedgerEntryPublisher} uses ({@code config.KafkaProducerConfig}'s
 * {@code kafkaTemplate} bean, backed by a producer factory with a {@code transactionIdPrefix}) - a
 * send here only ever happens from inside one of the three refund-saga listeners
 * ({@code RefundRequestedListener}, {@code RefundFailedListener}), each running inside the Kafka
 * transaction the listener container already opened, exactly like every other M7-era publisher in
 * this service. There is deliberately no {@code executeInTransaction(...)} here, for the same
 * reason {@code KafkaLedgerEntryPublisher} has none: that helper starts a producer-only
 * transaction, excluding the consumed offsets.
 *
 * <p>The TTL sweeper ({@code adapters.in.scheduler.ReservationTtlSweeper}) also calls
 * {@link #publishReservationReleased} - from OUTSIDE any listener container, so outside any
 * container-driven transaction. {@code KafkaTemplate#send} on a transactional template with no
 * transaction already open on the calling thread automatically wraps that single send in its own
 * local producer transaction (Spring Kafka's documented behaviour for a transactional template
 * used outside {@code @Transactional}), so this still produces a well-formed, fully committed
 * (or, on failure, cleanly aborted) transaction - just not one that also carries a consumed offset,
 * because the sweeper never consumed anything.
 */
@Component
public class KafkaRefundEventPublisher implements RefundEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaRefundEventPublisher.class);
    private static final String AGGREGATE_TYPE = "refund";
    private static final String SOURCE = "ledger";

    private static final String FUNDS_RESERVED_EVENT_TYPE = "refunds.funds-reserved.v1";
    private static final String REFUND_FAILED_EVENT_TYPE = "refunds.refund-failed.v1";
    private static final String RESERVATION_RELEASED_EVENT_TYPE = "refunds.reservation-released.v1";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RefundEventAvroFactory avroFactory;
    private final String fundsReservedTopic;
    private final String refundFailedTopic;
    private final String reservationReleasedTopic;

    public KafkaRefundEventPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            RefundEventAvroFactory avroFactory,
            @Value("${ledger.kafka.funds-reserved-topic}") String fundsReservedTopic,
            @Value("${ledger.kafka.refund-failed-topic}") String refundFailedTopic,
            @Value("${ledger.kafka.reservation-released-topic}") String reservationReleasedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroFactory = avroFactory;
        this.fundsReservedTopic = fundsReservedTopic;
        this.refundFailedTopic = refundFailedTopic;
        this.reservationReleasedTopic = reservationReleasedTopic;
    }

    @Override
    public void publishFundsReserved(
            RefundRequest request,
            java.util.UUID causationEventId,
            String traceId,
            String correlationId,
            MerchantBalance balanceAfter) {
        EventEnvelope envelope =
                EventEnvelope.causedBy(
                        causationEventId,
                        FUNDS_RESERVED_EVENT_TYPE,
                        1,
                        request.refundId().toString(),
                        AGGREGATE_TYPE,
                        SOURCE,
                        traceId,
                        correlationId);
        com.example.psp.common.events.avro.FundsReserved event = avroFactory.toFundsReserved(envelope, request);

        send(fundsReservedTopic, request.merchantId(), event, envelope);
        log.info(
                "Published refunds.funds-reserved.v1 refundId={} paymentId={} merchantId={} amount={} "
                        + "balanceAfter={} {}",
                request.refundId(),
                request.paymentId(),
                request.merchantId(),
                request.amount().amount(),
                balanceAfter.balance().amount(),
                balanceAfter.balance().currency());
    }

    @Override
    public void publishRefundFailedInsufficientBalance(
            RefundRequest request, java.util.UUID causationEventId, String traceId, String correlationId) {
        EventEnvelope envelope =
                EventEnvelope.causedBy(
                        causationEventId,
                        REFUND_FAILED_EVENT_TYPE,
                        1,
                        request.refundId().toString(),
                        AGGREGATE_TYPE,
                        SOURCE,
                        traceId,
                        correlationId);
        com.example.psp.common.events.avro.RefundFailed event =
                avroFactory.toRefundFailed(envelope, request, "INSUFFICIENT_BALANCE");

        send(refundFailedTopic, request.merchantId(), event, envelope);
        log.info(
                "Published refunds.refund-failed.v1 reason=INSUFFICIENT_BALANCE refundId={} paymentId={} "
                        + "merchantId={} amount={}",
                request.refundId(),
                request.paymentId(),
                request.merchantId(),
                request.amount().amount());
    }

    @Override
    public void publishReservationReleased(
            RefundSagaState state,
            String reason,
            java.util.UUID causationEventId,
            String traceId,
            String correlationId) {
        EventEnvelope envelope =
                causationEventId == null
                        ? EventEnvelope.root(
                                RESERVATION_RELEASED_EVENT_TYPE,
                                1,
                                state.refundId().toString(),
                                AGGREGATE_TYPE,
                                SOURCE,
                                traceId,
                                correlationId)
                        : EventEnvelope.causedBy(
                                causationEventId,
                                RESERVATION_RELEASED_EVENT_TYPE,
                                1,
                                state.refundId().toString(),
                                AGGREGATE_TYPE,
                                SOURCE,
                                traceId,
                                correlationId);
        com.example.psp.common.events.avro.ReservationReleased event =
                avroFactory.toReservationReleased(envelope, state, reason);

        send(reservationReleasedTopic, state.merchantId(), event, envelope);
        log.info(
                "Published refunds.reservation-released.v1 reason={} refundId={} paymentId={} "
                        + "merchantId={} amount={}",
                reason,
                state.refundId(),
                state.paymentId(),
                state.merchantId(),
                state.amount().amount());
    }

    private void send(String topic, String key, Object value, EventEnvelope envelope) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, value);
        // M15: see KafkaLedgerEntryPublisher's identical comment.
        record.headers()
                .add("event-id", envelope.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add("event-type", envelope.eventType().getBytes(StandardCharsets.UTF_8))
                .add("aggregate-id", envelope.aggregateId().getBytes(StandardCharsets.UTF_8));

        kafkaTemplate
                .send(record)
                .whenComplete(
                        (result, ex) -> {
                            if (ex != null) {
                                log.error(
                                        "Failed to append {} for refundId={} - the surrounding transaction "
                                                + "will abort",
                                        topic,
                                        envelope.aggregateId(),
                                        ex);
                            } else {
                                RecordMetadata metadata = result.getRecordMetadata();
                                log.debug(
                                        "Appended {} refundId={} partition={} offset={}",
                                        topic,
                                        envelope.aggregateId(),
                                        metadata.partition(),
                                        metadata.offset());
                            }
                        });
    }
}
