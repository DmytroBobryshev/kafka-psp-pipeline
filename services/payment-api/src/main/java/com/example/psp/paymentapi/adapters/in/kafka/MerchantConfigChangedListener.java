package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.paymentapi.application.MerchantViewProjectionUseCase;
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

    private final MerchantViewProjectionUseCase useCase;
    private final MerchantConfigChangedMapper mapper;

    public MerchantConfigChangedListener(
            MerchantViewProjectionUseCase useCase, MerchantConfigChangedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${payment-api.kafka.merchant-config-changed-topic}",
            containerFactory = "merchantViewKafkaListenerContainerFactory")
    public void onMessage(
            @Payload(required = false) MerchantConfigChanged event,
            @Header(KafkaHeaders.RECEIVED_KEY) String merchantId,
            Acknowledgment ack) {
        if (event == null) {
            log.info("Consumed merchant-config TOMBSTONE merchantId={}", merchantId);
            useCase.applyDelete(merchantId);
        } else {
            log.info(
                    "Consumed merchant-config merchantId={} status={}", merchantId, event.getStatus());
            useCase.applyUpsert(mapper.toCommand(event));
        }
        ack.acknowledge();
    }
}
