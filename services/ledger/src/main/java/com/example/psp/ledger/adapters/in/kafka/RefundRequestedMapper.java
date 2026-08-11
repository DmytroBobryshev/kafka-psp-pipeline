package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundRequested;
import com.example.psp.ledger.application.ReserveRefundCommand;
import com.example.psp.ledger.domain.model.Money;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the inbound Kafka hexagon boundary: event -&gt; command (ADR-0007). Same
 * shape as {@code PaymentStatusChangedMapper} (M9 Phase 2) - UUID-shaped Avro {@code string}
 * fields are converted explicitly, since the generated Avro record's {@code refundId}/
 * {@code paymentId}/{@code envelope.eventId} are plain strings.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundRequestedMapper {

    @Mapping(
            target = "inboundEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "refundId", expression = "java(java.util.UUID.fromString(event.getRefundId()))")
    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "amount", expression = "java(Money.of(event.getAmount(), event.getCurrency()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    ReserveRefundCommand toCommand(RefundRequested event);
}
