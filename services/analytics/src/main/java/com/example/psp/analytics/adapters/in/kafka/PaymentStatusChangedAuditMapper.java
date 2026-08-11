package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.analytics.domain.model.PaymentStatusAuditEntry;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the M13 batch listener's inbound boundary (ADR-0007): Avro {@code ->}
 * domain, one record at a time - {@code adapters.in.kafka.PaymentStatusChangedBatchListener} maps
 * the whole batch by applying this once per element.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusChangedAuditMapper {

    @Mapping(target = "eventId", source = "event.envelope.eventId")
    @Mapping(target = "paymentId", source = "event.paymentId")
    @Mapping(target = "merchantId", source = "event.merchantId")
    @Mapping(target = "status", source = "event.status")
    @Mapping(target = "occurredAt", source = "event.envelope.occurredAt")
    PaymentStatusAuditEntry toEntry(PaymentStatusChanged event);
}
