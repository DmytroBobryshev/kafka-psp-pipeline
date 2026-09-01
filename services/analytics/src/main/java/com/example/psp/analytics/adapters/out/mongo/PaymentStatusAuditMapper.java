package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.PaymentStatusAuditEntry;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusAuditMapper {

    @Mapping(target = "id", source = "eventId")
    PaymentStatusAuditDocument toDocument(PaymentStatusAuditEntry entry);
}
