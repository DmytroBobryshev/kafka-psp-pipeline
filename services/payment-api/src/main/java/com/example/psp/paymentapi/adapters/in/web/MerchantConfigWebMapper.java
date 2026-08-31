package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.UpsertMerchantConfigCommand;
import com.example.psp.paymentapi.domain.model.MerchantConfig;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the merchant-config web boundary (M10), same conventions as
 * {@link PaymentWebMapper}: {@code componentModel = "spring"}, {@code unmappedTargetPolicy =
 * ERROR} so a field added to the command or the response fails the build rather than silently
 * arriving as {@code null} on the topic - which on a compacted topic would be permanent, since
 * the broken snapshot becomes the retained last-value for that key.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MerchantConfigWebMapper {

    /**
     * {@code merchantId} comes from the URL path, everything else from the request body - hence
     * two source parameters rather than one DTO. See {@link UpsertMerchantConfigRequest} on why
     * the id is not in the body.
     */
    @Mapping(target = "merchantId", source = "merchantId")
    @Mapping(target = "displayName", source = "request.displayName")
    @Mapping(target = "status", source = "request.status")
    @Mapping(target = "payoutCurrency", source = "request.payoutCurrency")
    @Mapping(target = "allowedCurrencies", source = "request.allowedCurrencies")
    @Mapping(target = "webhookUrl", source = "request.webhookUrl")
    @Mapping(
            target = "declineRateAlertThresholdBps",
            source = "request.declineRateAlertThresholdBps")
    UpsertMerchantConfigCommand toCommand(String merchantId, UpsertMerchantConfigRequest request);

    /** Enum {@code ->} String is MapStruct's built-in conversion; named explicitly for grep-ability. */
    @Mapping(target = "status", expression = "java(config.status().name())")
    MerchantConfigResponse toResponse(MerchantConfig config);
}
