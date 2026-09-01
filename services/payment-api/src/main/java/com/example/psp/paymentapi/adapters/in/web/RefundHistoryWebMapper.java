package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.domain.model.RefundHistoryItem;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundHistoryWebMapper {

    RefundHistoryItemResponse toResponse(RefundHistoryItem item);
}
