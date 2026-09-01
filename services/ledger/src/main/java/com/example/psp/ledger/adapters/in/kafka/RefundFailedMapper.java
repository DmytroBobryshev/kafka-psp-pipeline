package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.ledger.application.ReleaseRefundCommand;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundFailedMapper {

    @Mapping(
            target = "inboundEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "refundId", expression = "java(java.util.UUID.fromString(event.getRefundId()))")
    @Mapping(target = "reason", source = "event.reason")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    ReleaseRefundCommand toCommand(RefundFailed event);
}
