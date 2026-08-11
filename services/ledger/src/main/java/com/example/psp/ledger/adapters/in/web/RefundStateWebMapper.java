package com.example.psp.ledger.adapters.in.web;

import com.example.psp.ledger.domain.model.RefundSagaState;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the web hexagon boundary: domain -&gt; response DTO (ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundStateWebMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    @Mapping(target = "status", expression = "java(state.status().name())")
    RefundStateResponse toResponse(RefundSagaState state);
}
