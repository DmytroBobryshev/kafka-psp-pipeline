package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
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
 *
 * <p>M9 Phase 2: {@code event} is now the generated Avro {@link PaymentStatusChanged} record,
 * whose UUID-shaped fields ({@code envelope.eventId}) are plain Avro {@code string} - hence the
 * explicit {@code UUID.fromString(...)} expression below, where the JSON-era record's
 * {@code envelope.eventId()} was already a {@code UUID}. Same pattern psp-connector's
 * {@code PaymentRequestedMapper} established in M9 Phase 1.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusChangedMapper {

    @Mapping(
            target = "inboundEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "amount", expression = "java(Money.of(event.getAmount(), event.getCurrency()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    RecordLedgerEntryCommand toCommand(PaymentStatusChanged event);
}
