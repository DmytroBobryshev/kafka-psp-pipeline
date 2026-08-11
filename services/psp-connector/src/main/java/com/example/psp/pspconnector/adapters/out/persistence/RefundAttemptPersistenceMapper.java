package com.example.psp.pspconnector.adapters.out.persistence;

import com.example.psp.pspconnector.domain.model.RefundAttempt;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the refund-path persistence hexagon boundary (M11, ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundAttemptPersistenceMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    @Mapping(target = "outcome", expression = "java(attempt.getOutcome().name())")
    RefundAttemptEntity toEntity(RefundAttempt attempt);
}
