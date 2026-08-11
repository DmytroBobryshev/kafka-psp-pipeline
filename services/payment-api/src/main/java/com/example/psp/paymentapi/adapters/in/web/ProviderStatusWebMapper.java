package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.domain.model.ProviderStatusResult;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the web hexagon boundary (ADR-0007): {@link ProviderStatusResult} -&gt;
 * {@link ProviderStatusResponse}. Field names and types already match 1:1, so no
 * {@code @Mapping} overrides are needed - MapStruct's default by-name strategy handles the whole
 * thing.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface ProviderStatusWebMapper {

    ProviderStatusResponse toResponse(ProviderStatusResult result);
}
