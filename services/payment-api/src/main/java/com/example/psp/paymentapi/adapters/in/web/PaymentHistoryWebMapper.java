package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.domain.model.PaymentHistoryItem;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the M20 history endpoint's web hexagon boundary (ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase. Every field maps straight across since M21 widened
 * {@link PaymentHistoryItem#status()} from {@code PaymentStatus} to a plain {@code String} - no
 * {@code @Mapping} overrides needed any more (the {@code .name()} expression this used through M20
 * is gone with it).
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentHistoryWebMapper {

    PaymentHistoryItemResponse toResponse(PaymentHistoryItem item);
}
