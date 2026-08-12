package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.pspconnector.domain.model.RefundAttempt;
import com.example.psp.pspconnector.domain.model.RefundOutcome;
import com.example.psp.pspconnector.domain.port.RefundStatusPublisher;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Real Kafka adapter for {@link RefundStatusPublisher} (M11). Publishes to
 * {@code refunds.refund-completed.v1} or {@code refunds.refund-failed.v1}, keyed by
 * {@code merchantId} (ADR-0003 - same key as {@code payments.payment-status-changed.v1}, for the
 * same reason: every refund-saga event for one merchant must land on one partition, in order, so
 * the ledger's compensation listener has a single in-flight writer per balance).
 */
@Component
public class KafkaRefundStatusPublisher implements RefundStatusPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaRefundStatusPublisher.class);
    private static final String AGGREGATE_TYPE = "refund";
    private static final String SOURCE = "psp-connector";

    private static final String REFUND_COMPLETED_EVENT_TYPE = "refunds.refund-completed.v1";
    private static final String REFUND_FAILED_EVENT_TYPE = "refunds.refund-failed.v1";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RefundStatusAvroEventFactory avroEventFactory;
    private final String refundCompletedTopic;
    private final String refundFailedTopic;

    public KafkaRefundStatusPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            RefundStatusAvroEventFactory avroEventFactory,
            @Value("${psp-connector.kafka.refund-completed-topic}") String refundCompletedTopic,
            @Value("${psp-connector.kafka.refund-failed-topic}") String refundFailedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.avroEventFactory = avroEventFactory;
        this.refundCompletedTopic = refundCompletedTopic;
        this.refundFailedTopic = refundFailedTopic;
    }

    @Override
    public void publishOutcome(RefundAttempt attempt) {
        boolean completed = attempt.getOutcome() == RefundOutcome.COMPLETED;
        String eventType = completed ? REFUND_COMPLETED_EVENT_TYPE : REFUND_FAILED_EVENT_TYPE;
        String topic = completed ? refundCompletedTopic : refundFailedTopic;

        EventEnvelope envelope =
                EventEnvelope.causedBy(
                        attempt.getCausationEventId(),
                        eventType,
                        1,
                        attempt.getRefundId().toString(),
                        AGGREGATE_TYPE,
                        SOURCE,
                        attempt.getTraceId(),
                        attempt.getCorrelationId());

        Object event =
                completed
                        ? avroEventFactory.toRefundCompleted(envelope, attempt)
                        : avroEventFactory.toRefundFailed(envelope, attempt);

        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, attempt.getMerchantId(), event);
        // M15: see KafkaPaymentStatusPublisher's identical comment - the traceparent header is
        // now injected by KafkaTemplate's observation instrumentation, not written by hand here.
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
                                        "Failed to publish {} for refundId={}",
                                        topic,
                                        attempt.getRefundId(),
                                        ex);
                            } else {
                                RecordMetadata metadata = result.getRecordMetadata();
                                log.info(
                                        "Published {} refundId={} paymentId={} merchantId={} outcome={} "
                                                + "partition={} offset={}",
                                        topic,
                                        attempt.getRefundId(),
                                        attempt.getPaymentId(),
                                        attempt.getMerchantId(),
                                        attempt.getOutcome(),
                                        metadata.partition(),
                                        metadata.offset());
                            }
                        });
    }
}
