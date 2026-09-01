package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.domain.model.RefundHistoryItem;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the M23 refund-history endpoint's web hexagon boundary (ADR-0007). Every
 * field maps straight across - identical shape to {@link PaymentHistoryWebMapper}.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundHistoryWebMapper {

    RefundHistoryItemResponse toResponse(RefundHistoryItem item);
}
