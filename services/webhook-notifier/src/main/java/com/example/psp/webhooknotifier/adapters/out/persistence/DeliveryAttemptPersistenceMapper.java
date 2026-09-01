package com.example.psp.webhooknotifier.adapters.out.persistence;

import com.example.psp.webhooknotifier.domain.model.DeliveryAttempt;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface DeliveryAttemptPersistenceMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "outcome", source = "outcome")
    DeliveryAttemptDocument toDocument(DeliveryAttempt attempt);
}
