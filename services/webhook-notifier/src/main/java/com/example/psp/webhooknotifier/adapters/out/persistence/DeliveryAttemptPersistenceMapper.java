package com.example.psp.webhooknotifier.adapters.out.persistence;

import com.example.psp.webhooknotifier.domain.model.DeliveryAttempt;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the persistence hexagon boundary (ADR-0007): domain -&gt; Mongo document.
 * {@code paymentId} converts {@code UUID -> String} via MapStruct's built-in conversion; {@code id}
 * is intentionally left for MongoDB to generate (ignored here, never set from the domain side -
 * this is an append-only log, never an update-by-id).
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface DeliveryAttemptPersistenceMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "outcome", source = "outcome")
    DeliveryAttemptDocument toDocument(DeliveryAttempt attempt);
}
