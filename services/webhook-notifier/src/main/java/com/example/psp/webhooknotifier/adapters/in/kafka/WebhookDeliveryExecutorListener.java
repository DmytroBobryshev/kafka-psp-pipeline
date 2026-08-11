package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.webhooknotifier.application.ExecuteWebhookDeliveryUseCase;
import com.example.psp.webhooknotifier.domain.model.RecordCoordinates;
import com.example.psp.webhooknotifier.domain.model.RetryEnvelope;
import com.example.psp.webhooknotifier.domain.model.RetryHeaderCodec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * The executor listener (topic-map's {@code webhook-notifier.executor.v1} group): ONE listener,
 * subscribed to the base delivery topic AND all three retry tiers, using the container factory
 * built in {@code config.KafkaConsumerConfig#executorKafkaListenerContainerFactory}. Which tier a
 * given record arrived on is read from the record itself ({@link ConsumerRecord#topic()}), not
 * from a separate listener method per topic - that is what lets
 * {@code domain.model.RetryChain#nextTierAfter} answer "what's next" from one place.
 *
 * <h2>Binding the whole {@code ConsumerRecord}, not {@code @Header}-per-field</h2>
 *
 * <p>The retry headers this service reads and writes ({@code domain.model.RetryHeaderNames}) are
 * written as plain UTF-8 bytes by {@code adapters.out.kafka.KafkaWebhookDeliveryPublisher} - the
 * same convention psp-connector's {@code KafkaPaymentStatusPublisher} uses for
 * {@code traceparent}/{@code event-id}. Reading them back explicitly via
 * {@link ConsumerRecord#headers()} (rather than relying on Spring's {@code @Header} argument
 * binding, which is tuned for headers written by its own {@code KafkaHeaderMapper}) keeps the
 * read side an exact, unsurprising mirror of the write side.
 *
 * <h2>The ack is NOT called here</h2>
 *
 * <p>See {@code application.ExecuteWebhookDeliveryUseCase}'s javadoc for why: the use case's
 * returned future only completes once this record's outcome has been durably handed off
 * (delivered, DLQ'd, or a retry hop successfully published/scheduled), and only then is it safe
 * to advance this consumer group's offset past it.
 */
@Component
public class WebhookDeliveryExecutorListener {

    private static final Logger log = LoggerFactory.getLogger(WebhookDeliveryExecutorListener.class);

    private final ExecuteWebhookDeliveryUseCase useCase;
    private final WebhookDeliveryRequestedMapper mapper;

    public WebhookDeliveryExecutorListener(
            ExecuteWebhookDeliveryUseCase useCase, WebhookDeliveryRequestedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = {
                "${webhook-notifier.kafka.delivery-requested-topic}",
                "${webhook-notifier.kafka.retry-5s-topic}",
                "${webhook-notifier.kafka.retry-1m-topic}",
                "${webhook-notifier.kafka.retry-15m-topic}"
            },
            containerFactory = "executorKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, WebhookDeliveryRequestedEvent> record, Acknowledgment ack) {
        RetryEnvelope envelope = RetryHeaderCodec.decode(name -> headerAsString(record, name));
        RecordCoordinates coordinates =
                new RecordCoordinates(record.topic(), record.partition(), record.offset(), Instant.ofEpochMilli(record.timestamp()));

        log.info(
                "Consumed webhook-delivery-requested paymentId={} merchantId={} topic={} attempt={}",
                record.value().paymentId(),
                record.value().merchantId(),
                record.topic(),
                envelope.attemptCount());

        useCase.execute(mapper.toCommand(record.value()), coordinates, envelope)
                .whenComplete(
                        (result, ex) -> {
                            if (ex == null) {
                                ack.acknowledge();
                            } else {
                                // Deliberately NOT acknowledged: the handoff to the next hop (or the
                                // DLQ) failed - most plausibly the Kafka broker itself is unreachable.
                                // Leaving the offset uncommitted means this record is redelivered
                                // (crash-restart or the next poll on this same consumer, depending on
                                // the failure) and the whole attempt - including the HTTP call - runs
                                // again. That duplicate HTTP attempt is why every retryable operation
                                // in this system is expected to be idempotent on the far side (ADR-0006);
                                // this service does not control the merchant's own idempotency, so this
                                // is a documented, accepted limitation, not a bug.
                                log.error(
                                        "Failed to hand off webhook-delivery-requested paymentId={} merchantId={} "
                                                + "topic={} - NOT acknowledging, record will be redelivered",
                                        record.value().paymentId(),
                                        record.value().merchantId(),
                                        record.topic(),
                                        ex);
                            }
                        });
    }

    private static String headerAsString(ConsumerRecord<String, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
