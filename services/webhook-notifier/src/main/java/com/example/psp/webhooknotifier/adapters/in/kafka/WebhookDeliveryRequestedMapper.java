package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.WebhookDeliveryRequested;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the executor's inbound Kafka boundary: event -&gt; domain command
 * (ADR-0007). Kept as an explicit mapper so {@code application/} depends only on
 * {@code domain.model.WebhookDeliveryCommand}, never on a Kafka wire-shape type.
 *
 * <p>M9 Phase 2: {@code event} is now the generated Avro {@link WebhookDeliveryRequested} record
 * (replacing the hand-written {@code adapters.in.kafka.WebhookDeliveryRequestedEvent}), whose
 * UUID-shaped fields are plain Avro {@code string} - hence the explicit
 * {@code UUID.fromString(...)} expressions, same pattern as every other M9 Phase 2 inbound mapper.
 */
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
