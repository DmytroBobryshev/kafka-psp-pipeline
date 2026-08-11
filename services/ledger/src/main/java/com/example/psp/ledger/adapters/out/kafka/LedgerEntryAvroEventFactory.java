package com.example.psp.ledger.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import org.springframework.stereotype.Component;

/**
 * Builds the Avro wire record for {@code ledger.ledger-entry-recorded.v1} (M9 Phase 2) from the
 * hand-written {@link EventEnvelope}, the {@link LedgerEntry} that was just applied, and the
 * {@link MerchantBalance} it produced.
 *
 * <p>A plain method, not a MapStruct {@code @Mapper} - the same deliberate exception
 * {@code payment-api}'s {@code PaymentAvroEventFactory} (M9 Phase 1) and psp-connector's
 * {@code PaymentStatusAvroEventFactory} (M9 Phase 2) both establish: the generated Avro builder is
 * reached through {@code newBuilder()}/{@code build()}, not a MapStruct-recognised setter
 * convention worth an annotation-processed interface for a handful of fields.
 */
@Component
public class LedgerEntryAvroEventFactory {

    /**
     * @param envelope     the JSON-era {@link EventEnvelope} {@link KafkaLedgerEntryPublisher}
     *                     already builds for every outbound event (unchanged).
     * @param entry        the ledger entry that was just applied.
     * @param balanceAfter the merchant balance snapshot the entry produced.
     * @return the Avro record ready to hand to {@code KafkaTemplate#send}.
     */
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
