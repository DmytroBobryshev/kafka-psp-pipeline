package com.example.psp.webhooknotifier.adapters.out.kafka;

import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface WebhookDeliveryEventMapper {

    WebhookDeliveryRequested toEvent(WebhookDeliveryCommand command);

    WebhookDeliveryCommand toCommand(WebhookDeliveryRequested event);
}
