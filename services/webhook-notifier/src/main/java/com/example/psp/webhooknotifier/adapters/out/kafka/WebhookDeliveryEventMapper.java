package com.example.psp.webhooknotifier.adapters.out.kafka;

import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the outbound Kafka boundary: domain command -&gt; wire event (ADR-0007).
 * Field names match exactly - a straight pass-through, kept explicit for the same reason as
 * {@code adapters.in.kafka.WebhookDeliveryRequestedMapper}.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface WebhookDeliveryEventMapper {

    WebhookDeliveryRequested toEvent(WebhookDeliveryCommand command);

    WebhookDeliveryCommand toCommand(WebhookDeliveryRequested event);
}
