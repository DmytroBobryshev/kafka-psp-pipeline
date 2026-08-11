package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.common.events.avro.FundsReserved;
import com.example.psp.pspconnector.application.ExecuteRefundCommand;
import com.example.psp.pspconnector.domain.model.Money;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the inbound Kafka hexagon boundary: event -&gt; command (ADR-0007). Same
 * shape as {@code PaymentRequestedMapper}'s Avro overload (M9 Phase 1) - UUID-shaped Avro
 * {@code string} fields converted explicitly.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface FundsReservedMapper {

    @Mapping(target = "refundId", expression = "java(java.util.UUID.fromString(event.getRefundId()))")
    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "amount", expression = "java(Money.of(event.getAmount(), event.getCurrency()))")
    @Mapping(
            target = "causationEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    ExecuteRefundCommand toCommand(FundsReserved event);
}
