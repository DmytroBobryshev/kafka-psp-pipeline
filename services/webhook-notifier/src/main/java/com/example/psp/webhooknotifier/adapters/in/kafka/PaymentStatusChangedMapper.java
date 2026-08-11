package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the planner's inbound Kafka boundary: event -&gt; domain command (ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - the same rule as
 * every other boundary mapper in this codebase.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusChangedMapper {

    @Mapping(target = "causationEventId", source = "event.envelope.eventId")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    WebhookDeliveryCommand toCommand(PaymentStatusChangedEvent event);
}
