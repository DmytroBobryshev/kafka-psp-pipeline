package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.domain.model.ProviderStatusResult;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface ProviderStatusWebMapper {

    ProviderStatusResponse toResponse(ProviderStatusResult result);
}
