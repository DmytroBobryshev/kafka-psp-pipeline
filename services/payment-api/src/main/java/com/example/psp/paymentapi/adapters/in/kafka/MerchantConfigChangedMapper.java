package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.paymentapi.application.UpsertMerchantViewCommand;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at {@code MerchantConfigChangedListener}'s inbound boundary: Avro event ->
 * application command. Only the upsert case - a tombstone carries no Avro record at all, so the
 * listener never calls this for a delete.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MerchantConfigChangedMapper {

    @Mapping(target = "status", source = "status", qualifiedByName = "toMerchantStatus")
    @Mapping(target = "updatedAt", source = "envelope.occurredAt")
    UpsertMerchantViewCommand toCommand(MerchantConfigChanged event);

    /** Throws on a value this schema's own doc says is exhaustive - a contract violation
     * (ADR-0006 category C/D), not a status this method should guess at. */
    @Named("toMerchantStatus")
    default MerchantStatus toMerchantStatus(String status) {
        return MerchantStatus.valueOf(status);
    }
}
