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
