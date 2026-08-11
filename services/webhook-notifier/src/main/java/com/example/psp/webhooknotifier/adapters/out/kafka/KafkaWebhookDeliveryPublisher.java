package com.example.psp.webhooknotifier.adapters.out.kafka;

import com.example.psp.webhooknotifier.domain.model.RetryChain;
import com.example.psp.webhooknotifier.domain.model.RetryEnvelope;
import com.example.psp.webhooknotifier.domain.model.RetryHeaderCodec;
import com.example.psp.webhooknotifier.domain.model.RetryHeaderNames;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import com.example.psp.webhooknotifier.domain.port.WebhookDeliveryPublisher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

/**
 * Real Kafka adapter for {@link WebhookDeliveryPublisher}. Every record is keyed by
 * {@code command.merchantId()} (ADR-0003, matches docs/diagrams/topic-map.md's key for the whole
 * webhook delivery chain) and carries {@link RetryEnvelope} encoded as headers via
 * {@code domain.model.RetryHeaderCodec} - see {@code domain.model.RetryHeaderNames} for exactly
 * which headers and what each means.
 *
 * <h2>Why {@link #publishDelayed} does not block</h2>
 *
 * <p>{@link TaskScheduler#schedule(Runnable, Instant)} hands the delayed send to a background
 * scheduler thread (a small pool, {@code config.KafkaConsumerConfig}'s
 * {@code webhookRetryTaskScheduler} bean) and returns immediately - the calling thread (a Kafka
 * consumer poll thread, via {@code adapters.in.kafka.WebhookDeliveryExecutorListener}) is never
 * held. This is THE non-blocking property M8 exists to teach: if this method instead called
 * {@code Thread.sleep(delay)} before sending, that sleep would happen on the listener's own
 * thread, which is the SAME thread the container calls {@code poll()} from between records. A
 * sleep anywhere near the tens-of-seconds-to-minutes range used here would blow
 * {@code max.poll.interval.ms} - psp-connector's M4 "prove it" experiment measured exactly this
 * failure mode with an even SHORTER, ~10s stall: 16 rebalances in the observed run, the consumer
 * group's coordinator concluding the member was stuck and evicting it, over and over, with almost
 * no actual progress (see services/psp-connector/README.md's "Prove it" section, the row
 * documenting 16 rebalances). A 15-minute blocking sleep would not cause a rebalance storm - it
 * would cause a PERMANENT one, since the member would never return from a single {@code poll()}
 * call long enough to heartbeat-check back in. Scheduling instead of sleeping is what keeps the
 * Kafka poll loop free to keep polling and heartbeating while a retry's clock runs down elsewhere.
 */
@Component
public class KafkaWebhookDeliveryPublisher implements WebhookDeliveryPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaWebhookDeliveryPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final WebhookDeliveryEventMapper mapper;
    private final TaskScheduler taskScheduler;
    private final RetryChain retryChain;

    public KafkaWebhookDeliveryPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            WebhookDeliveryEventMapper mapper,
            TaskScheduler webhookRetryTaskScheduler,
            RetryChain retryChain) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
        this.taskScheduler = webhookRetryTaskScheduler;
        this.retryChain = retryChain;
    }

    @Override
    public CompletableFuture<Void> publishNow(String topic, WebhookDeliveryCommand command, RetryEnvelope envelope) {
        return send(topic, command, envelope);
    }

    @Override
    public CompletableFuture<Void> publishDelayed(
            String topic, WebhookDeliveryCommand command, RetryEnvelope envelope, Duration delay) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        taskScheduler.schedule(
                () ->
                        send(topic, command, envelope)
                                .whenComplete(
                                        (v, ex) -> {
                                            if (ex == null) {
                                                result.complete(null);
                                            } else {
                                                result.completeExceptionally(ex);
                                            }
                                        }),
                Instant.now().plus(delay));
        return result;
    }

    private CompletableFuture<Void> send(String topic, WebhookDeliveryCommand command, RetryEnvelope envelope) {
        WebhookDeliveryRequested event = mapper.toEvent(command);
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, command.merchantId(), event);

        RetryHeaderCodec.encode(envelope)
                .forEach((name, value) -> record.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));

        if (topic.equals(retryChain.dlqTopic())) {
            // x-failed-at: only meaningful on the terminal DLQ record, per ADR-0006.
            record.headers()
                    .add(RetryHeaderNames.FAILED_AT, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        }

        CompletableFuture<Void> result = new CompletableFuture<>();
        kafkaTemplate
                .send(record)
                .whenComplete(
                        (sendResult, ex) -> {
                            if (ex != null) {
                                log.error(
                                        "Failed to publish to {} paymentId={} merchantId={}",
                                        topic,
                                        command.paymentId(),
                                        command.merchantId(),
                                        ex);
                                result.completeExceptionally(ex);
                            } else {
                                log.info(
                                        "Published to {} paymentId={} merchantId={} attempt={} partition={} offset={}",
                                        topic,
                                        command.paymentId(),
                                        command.merchantId(),
                                        envelope.attemptCount(),
                                        sendResult.getRecordMetadata().partition(),
                                        sendResult.getRecordMetadata().offset());
                                result.complete(null);
                            }
                        });
        return result;
    }
}
