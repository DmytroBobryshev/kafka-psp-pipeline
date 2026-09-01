package com.example.psp.ledger.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import org.springframework.stereotype.Component;

@Component
public class LedgerEntryAvroEventFactory {

    public com.example.psp.common.events.avro.LedgerEntryRecorded toAvro(
            EventEnvelope envelope, LedgerEntry entry, MerchantBalance balanceAfter) {
        com.example.psp.common.events.avro.EventEnvelope avroEnvelope =
                com.example.psp.common.events.avro.EventEnvelope.newBuilder()
                        .setEventId(envelope.eventId().toString())
                        .setEventType(envelope.eventType())
                        .setEventVersion(envelope.eventVersion())
                        .setAggregateId(envelope.aggregateId())
                        .setAggregateType(envelope.aggregateType())
                        .setOccurredAt(envelope.occurredAt())
                        .setSource(envelope.source())
                        .setTraceId(envelope.traceId())
                        .setCorrelationId(envelope.correlationId())
                        .setCausationId(envelope.causationId() == null ? null : envelope.causationId().toString())
                        .build();

        return com.example.psp.common.events.avro.LedgerEntryRecorded.newBuilder()
                .setEnvelope(avroEnvelope)
                .setEntryId(entry.getId().toString())
                .setMerchantId(entry.getMerchantId())
                .setPaymentId(entry.getPaymentId().toString())
                .setDirection(entry.getDirection().name())
                .setAmount(entry.getAmount().amount())
                .setCurrency(entry.getAmount().currency())
                .setBalanceAfter(balanceAfter.balance().amount())
                .setRecordedAt(entry.getRecordedAt())
                .build();
    }
}
