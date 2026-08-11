package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.PaymentStatusAuditEntry;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the persistence boundary (ADR-0007): domain {@code ->} MongoDB document.
 * Same convention as every other projection mapper in this service - {@code
 * unmappedTargetPolicy = ERROR}.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusAuditMapper {

    @Mapping(target = "id", source = "eventId")
    PaymentStatusAuditDocument toDocument(PaymentStatusAuditEntry entry);
}
