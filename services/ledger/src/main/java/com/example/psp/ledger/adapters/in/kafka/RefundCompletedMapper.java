package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.ledger.application.SettleRefundCommand;
import com.example.psp.ledger.domain.model.Money;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/** MapStruct mapper at the inbound Kafka hexagon boundary: event -&gt; command (ADR-0007). */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundCompletedMapper {

    @Mapping(
            target = "inboundEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "refundId", expression = "java(java.util.UUID.fromString(event.getRefundId()))")
    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "amount", expression = "java(Money.of(event.getAmount(), event.getCurrency()))")
    @Mapping(target = "providerReference", source = "event.providerReference")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    SettleRefundCommand toCommand(RefundCompleted event);
}
