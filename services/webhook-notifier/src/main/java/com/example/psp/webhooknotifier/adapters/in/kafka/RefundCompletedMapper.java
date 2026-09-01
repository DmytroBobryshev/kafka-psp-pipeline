package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundCompletedMapper {

    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "refundId", expression = "java(java.util.UUID.fromString(event.getRefundId()))")
    @Mapping(target = "status", constant = "COMPLETED")
    @Mapping(target = "declineReason", ignore = true)
    @Mapping(
            target = "causationEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    @Mapping(target = "eventType", constant = "REFUND_COMPLETED")
    WebhookDeliveryCommand toCommand(RefundCompleted event);
}
