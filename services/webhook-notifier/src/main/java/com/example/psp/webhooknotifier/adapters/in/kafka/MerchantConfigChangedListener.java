package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.webhooknotifier.application.MerchantWebhookProjectionUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * The merchant-webhook projection listener (group {@code webhook-notifier.merchant-view.v1},
 * {@code config.KafkaConsumerConfig#merchantViewKafkaListenerContainerFactory}): mirrors
 * {@code merchants.merchant-config-changed.v1} into {@code merchant_webhooks} so
 * {@code adapters.out.http.RestClientMerchantWebhookClient} can resolve a merchant's REAL
 * webhookUrl at delivery time instead of always falling back to the simulated endpoint - the M8
 * bug this listener fixes. A fresh consumer group replays the whole compacted log from
 * {@code auto-offset-reset=earliest} (the service default, {@code application.yml}), so this
 * projection is complete on first startup, not just from the next config change onward.
 *
 * <p>Same {@code @Payload(required = false)}-for-tombstone idiom as payment-api's own
 * {@code MerchantConfigChangedListener}, which independently projects this same compacted topic
 * into its own read model (ADR-0005: a separate consumer group and a separate database, never
 * shared state between services).
 *
 * <p>No DLQ - a derived, lossy read-model projection (ADR-0006), same reasoning as
 * {@link PaymentStatusChangedListener}: a record this listener cannot process is logged and
 * skipped by the container's zero-retry error handler.
 */
@Component
public class MerchantConfigChangedListener {

    private static final Logger log = LoggerFactory.getLogger(MerchantConfigChangedListener.class);

    private final MerchantWebhookProjectionUseCase useCase;

    public MerchantConfigChangedListener(MerchantWebhookProjectionUseCase useCase) {
        this.useCase = useCase;
    }

    @KafkaListener(
            topics = "${webhook-notifier.kafka.merchant-config-changed-topic}",
            containerFactory = "merchantViewKafkaListenerContainerFactory")
    public void onMessage(
            @Payload(required = false) MerchantConfigChanged event,
            @Header(KafkaHeaders.RECEIVED_KEY) String merchantId,
            Acknowledgment ack) {
        if (event == null) {
            log.info("Consumed merchant-config TOMBSTONE merchantId={} - removing webhook projection", merchantId);
            useCase.applyDelete(merchantId);
        } else {
            log.info(
                    "Consumed merchant-config merchantId={} webhookUrl={}", merchantId, event.getWebhookUrl());
            useCase.applyUpsert(merchantId, event.getWebhookUrl(), event.getEnvelope().getOccurredAt());
        }
        ack.acknowledge();
    }
}
