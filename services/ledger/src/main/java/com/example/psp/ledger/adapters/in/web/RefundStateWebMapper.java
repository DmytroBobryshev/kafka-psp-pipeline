package com.example.psp.ledger.adapters.in.web;

import com.example.psp.ledger.domain.model.RefundSagaState;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundStateWebMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    @Mapping(target = "status", expression = "java(state.status().name())")
    RefundStateResponse toResponse(RefundSagaState state);
}
