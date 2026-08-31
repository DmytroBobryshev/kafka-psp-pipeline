package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.domain.model.MerchantView;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/** MapStruct mapper at the merchant-query web boundary: {@link MerchantView} -> {@link MerchantResponse}. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MerchantViewWebMapper {

    @Mapping(target = "status", expression = "java(view.status().name())")
    MerchantResponse toResponse(MerchantView view);
}
