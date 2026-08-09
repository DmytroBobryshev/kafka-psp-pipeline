package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.pspconnector.application.ProcessPaymentRequestCommand;
import com.example.psp.pspconnector.domain.model.Money;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the inbound Kafka hexagon boundary: event -&gt; command (ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentRequestedMapper {

    @Mapping(target = "amount", expression = "java(Money.of(event.amount(), event.currency()))")
    @Mapping(target = "causationEventId", source = "event.envelope.eventId")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    ProcessPaymentRequestCommand toCommand(PaymentRequestedEvent event);
}
