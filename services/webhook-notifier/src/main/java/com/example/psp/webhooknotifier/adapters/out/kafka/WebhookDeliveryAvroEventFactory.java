package com.example.psp.webhooknotifier.adapters.out.kafka;

import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.springframework.stereotype.Component;

@Component
public class WebhookDeliveryAvroEventFactory {

    public com.example.psp.common.events.avro.WebhookDeliveryRequested toAvro(WebhookDeliveryCommand command) {
        return com.example.psp.common.events.avro.WebhookDeliveryRequested.newBuilder()
                .setPaymentId(command.paymentId().toString())
                .setMerchantId(command.merchantId())
                .setAmount(command.amount())
                .setCurrency(command.currency())
                .setStatus(command.status())
                .setDeclineReason(command.declineReason())
                .setCausationEventId(command.causationEventId().toString())
                .setTraceId(command.traceId())
                .setCorrelationId(command.correlationId())
                .setEventType(command.eventType())
                .setRefundId(command.refundId() == null ? null : command.refundId().toString())
                .build();
    }
}
