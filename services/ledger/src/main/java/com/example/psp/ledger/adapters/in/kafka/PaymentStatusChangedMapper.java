package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.ledger.application.RecordLedgerEntryCommand;
import com.example.psp.ledger.domain.model.Money;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

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
