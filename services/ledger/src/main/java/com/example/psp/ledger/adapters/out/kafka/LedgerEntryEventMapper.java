package com.example.psp.ledger.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.ledger.domain.model.LedgerEntry;
import com.example.psp.ledger.domain.model.MerchantBalance;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the outbound Kafka hexagon boundary: domain -&gt; event (ADR-0007). The
 * {@link EventEnvelope} is built by the caller ({@link KafkaLedgerEntryPublisher}) and handed in as
 * a source parameter - same pattern as {@code psp-connector}'s {@code PaymentStatusEventMapper},
 * for the same reason: the envelope needs runtime causation context that no single domain object
 * carries on its own.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface LedgerEntryEventMapper {

    @Mapping(target = "envelope", source = "envelope")
    @Mapping(target = "entryId", source = "entry.id")
    @Mapping(target = "merchantId", source = "entry.merchantId")
    @Mapping(target = "paymentId", source = "entry.paymentId")
    @Mapping(target = "direction", expression = "java(entry.getDirection().name())")
    @Mapping(target = "amount", source = "entry.amount.amount")
    @Mapping(target = "currency", source = "entry.amount.currency")
    @Mapping(target = "recordedAt", source = "entry.recordedAt")
    @Mapping(target = "balanceAfter", source = "balanceAfter.balance.amount")
    LedgerEntryRecorded toEvent(
            EventEnvelope envelope, LedgerEntry entry, MerchantBalance balanceAfter);
}
