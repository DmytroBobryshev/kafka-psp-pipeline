package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.RequestRefundCommand;
import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Refund;
import java.util.UUID;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the refund web hexagon boundary (M11, ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundWebMapper {

    @Mapping(target = "paymentId", source = "paymentId")
    @Mapping(target = "amount", expression = "java(new Money(request.amount(), request.currency()))")
    @Mapping(target = "reason", source = "request.reason")
    RequestRefundCommand toCommand(UUID paymentId, RequestRefundRequest request);

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    @Mapping(target = "status", constant = "REQUESTED")
    RefundResponse toResponse(Refund refund);
}
