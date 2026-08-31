package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.domain.model.PaymentHistoryItem;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the M20 history endpoint's web hexagon boundary (ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentHistoryWebMapper {

    @Mapping(target = "status", expression = "java(item.status().name())")
    PaymentHistoryItemResponse toResponse(PaymentHistoryItem item);
}
