package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.ledger.application.RecordLedgerEntryCommand;
import com.example.psp.ledger.domain.model.Money;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the inbound Kafka hexagon boundary: event -&gt; command (ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase.
 *
 * <p>{@code inboundEventId} is mapped from {@code envelope.eventId} rather than from anything in
 * the payload: it is the ADR-0002 idempotency key and the only field on the wire that is stable
 * across replays, rebalances, aborted transactions and offset resets.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusChangedMapper {

    @Mapping(target = "inboundEventId", source = "event.envelope.eventId")
    @Mapping(target = "amount", expression = "java(Money.of(event.amount(), event.currency()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    RecordLedgerEntryCommand toCommand(PaymentStatusChangedEvent event);
}
