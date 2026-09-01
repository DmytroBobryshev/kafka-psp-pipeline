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
