package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.paymentapi.application.UpsertMerchantViewCommand;
import com.example.psp.paymentapi.domain.model.MerchantStatus;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MerchantConfigChangedMapper {

    @Mapping(target = "status", source = "status", qualifiedByName = "toMerchantStatus")
    @Mapping(target = "updatedAt", source = "envelope.occurredAt")
    UpsertMerchantViewCommand toCommand(MerchantConfigChanged event);

    @Named("toMerchantStatus")
    default MerchantStatus toMerchantStatus(String status) {
        return MerchantStatus.valueOf(status);
    }
}
