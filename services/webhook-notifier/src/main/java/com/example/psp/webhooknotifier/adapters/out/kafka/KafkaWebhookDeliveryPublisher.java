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
import org.springframework.beans.factory.annotation.Qualifier;
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
 * <h2>M9 Phase 2 - two templates, one publisher</h2>
 *
 * <p>{@code webhooks.webhook-delivery-requested.v2} and its three retry tiers are now Avro; the
 * terminal {@code .v2.dlq} deliberately stays JSON (byte-tolerant) - see
 * {@code config.KafkaProducerConfig}'s javadoc for the full reasoning:
 * {@code io.confluent.kafka.serializers.KafkaAvroSerializer} cannot serialize the raw {@code byte[]}
 * {@code DeadLetterPublishingRecoverer} republishes for a genuine poison pill (it needs an Avro
 * schema for whatever it is handed), whereas Spring Kafka's own {@code JsonSerializer} special-cases
 * a {@code byte[]} value and writes it through unchanged - the exact mechanism M8's "Poison pill
 * proof" depends on. This class therefore holds two {@link KafkaTemplate} beans and picks one per
 * {@link #send}, by destination topic: everything bound for {@link RetryChain#dlqTopic()} goes out
 * through {@code webhookDeliveryDlqKafkaTemplate} using the hand-written JSON
 * {@code adapters.out.kafka.WebhookDeliveryRequested} record (unchanged since M8); everything else
 * goes out through {@code webhookDeliveryAvroKafkaTemplate} using the generated Avro record from
 * {@link WebhookDeliveryAvroEventFactory}. A DLQ replay ({@code application.ReplayDlqUseCase})
 * republishes onto {@link RetryChain#baseTopic()}, so it naturally takes the Avro path even though
 * the record it read came off the JSON DLQ - the same "read tolerant, write governed" shape M9
 * Phase 1's outbox adapter uses at a different boundary.
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

    private final KafkaTemplate<String, Object> avroKafkaTemplate;
    private final KafkaTemplate<String, Object> dlqKafkaTemplate;
    private final WebhookDeliveryEventMapper mapper;
    private final WebhookDeliveryAvroEventFactory avroEventFactory;
    private final TaskScheduler taskScheduler;
    private final RetryChain retryChain;

    public KafkaWebhookDeliveryPublisher(
            @Qualifier("webhookDeliveryAvroKafkaTemplate") KafkaTemplate<String, Object> avroKafkaTemplate,
            @Qualifier("webhookDeliveryDlqKafkaTemplate") KafkaTemplate<String, Object> dlqKafkaTemplate,
            WebhookDeliveryEventMapper mapper,
            WebhookDeliveryAvroEventFactory avroEventFactory,
            TaskScheduler webhookRetryTaskScheduler,
            RetryChain retryChain) {
        this.avroKafkaTemplate = avroKafkaTemplate;
        this.dlqKafkaTemplate = dlqKafkaTemplate;
        this.mapper = mapper;
        this.avroEventFactory = avroEventFactory;
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
        boolean dlqBound = topic.equals(retryChain.dlqTopic());

        // M9 Phase 2: the DLQ stays on the hand-written JSON record + the byte-tolerant template;
        // the base topic and every retry tier go out as the generated Avro record instead - see
        // this class's javadoc.
        Object event = dlqBound ? mapper.toEvent(command) : avroEventFactory.toAvro(command);
        KafkaTemplate<String, Object> template = dlqBound ? dlqKafkaTemplate : avroKafkaTemplate;

        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, command.merchantId(), event);

        RetryHeaderCodec.encode(envelope)
                .forEach((name, value) -> record.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));

        if (dlqBound) {
            // x-failed-at: only meaningful on the terminal DLQ record, per ADR-0006.
            record.headers()
                    .add(RetryHeaderNames.FAILED_AT, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        }

        CompletableFuture<Void> result = new CompletableFuture<>();
        template
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
