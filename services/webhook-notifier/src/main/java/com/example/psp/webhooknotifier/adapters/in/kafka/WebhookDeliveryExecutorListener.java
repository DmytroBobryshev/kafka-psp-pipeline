package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.WebhookDeliveryRequested;
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
    public void onMessage(ConsumerRecord<String, WebhookDeliveryRequested> record, Acknowledgment ack) {
        RetryEnvelope envelope = RetryHeaderCodec.decode(name -> headerAsString(record, name));
        RecordCoordinates coordinates =
                new RecordCoordinates(record.topic(), record.partition(), record.offset(), Instant.ofEpochMilli(record.timestamp()));

        log.info(
                "Consumed webhook-delivery-requested paymentId={} merchantId={} topic={} attempt={}",
                record.value().getPaymentId(),
                record.value().getMerchantId(),
                record.topic(),
                envelope.attemptCount());

        useCase.execute(mapper.toCommand(record.value()), coordinates, envelope)
                .whenComplete(
                        (result, ex) -> {
                            if (ex == null) {
                                ack.acknowledge();
                            } else {
                                log.error(
                                        "Failed to hand off webhook-delivery-requested paymentId={} merchantId={} "
                                                + "topic={} - NOT acknowledging, record will be redelivered",
                                        record.value().getPaymentId(),
                                        record.value().getMerchantId(),
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
