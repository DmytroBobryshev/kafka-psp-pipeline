package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.pspconnector.application.ProcessPaymentRequestCommand;
import com.example.psp.pspconnector.domain.model.Money;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentRequestedMapper {

    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "amount", expression = "java(Money.of(event.getAmount(), event.getCurrency()))")
    @Mapping(
            target = "causationEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    ProcessPaymentRequestCommand toCommand(PaymentRequested event);

    @Mapping(target = "amount", expression = "java(Money.of(event.amount(), event.currency()))")
    @Mapping(target = "causationEventId", source = "event.envelope.eventId")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    ProcessPaymentRequestCommand toCommand(PaymentRequestedEvent event);
}
