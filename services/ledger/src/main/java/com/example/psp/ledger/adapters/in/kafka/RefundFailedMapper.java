package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.ledger.application.ReleaseRefundCommand;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the inbound Kafka hexagon boundary: event -&gt; command (ADR-0007).
 * Deliberately does NOT map {@code amount}/{@code merchantId}/{@code paymentId} - the compensation
 * use case releases against the reservation this ledger itself already recorded, not against
 * whatever the failed event happens to carry (see {@code application.ReleaseRefundUseCase}).
 */
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
