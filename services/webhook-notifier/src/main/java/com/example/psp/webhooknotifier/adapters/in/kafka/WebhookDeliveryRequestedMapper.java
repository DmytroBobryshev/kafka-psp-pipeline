package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.WebhookDeliveryRequested;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface WebhookDeliveryRequestedMapper {

    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(
            target = "causationEventId",
            expression = "java(java.util.UUID.fromString(event.getCausationEventId()))")
    // M19: refundId is nullable on the wire (Avro ["null","string"]) - only wrap it when present.
    @Mapping(
            target = "refundId",
            expression =
                    "java(event.getRefundId() == null ? null : java.util.UUID.fromString(event.getRefundId()))")
    WebhookDeliveryCommand toCommand(WebhookDeliveryRequested event);
}
