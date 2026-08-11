package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the executor's inbound Kafka boundary: event -&gt; domain command
 * (ADR-0007). Field names match exactly, so this is a straight pass-through - kept as an explicit
 * mapper anyway (rather than reusing the record directly) so {@code application/} depends only on
 * {@code domain.model.WebhookDeliveryCommand}, never on a Kafka wire-shape type.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface WebhookDeliveryRequestedMapper {

    WebhookDeliveryCommand toCommand(WebhookDeliveryRequestedEvent event);
}
