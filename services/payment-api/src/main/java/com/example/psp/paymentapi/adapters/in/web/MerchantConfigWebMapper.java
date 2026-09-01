package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.UpsertMerchantConfigCommand;
import com.example.psp.paymentapi.domain.model.MerchantConfig;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MerchantConfigWebMapper {

    @Mapping(target = "merchantId", source = "merchantId")
    @Mapping(target = "displayName", source = "request.displayName")
    @Mapping(target = "status", source = "request.status")
    @Mapping(target = "payoutCurrency", source = "request.payoutCurrency")
    @Mapping(target = "allowedCurrencies", source = "request.allowedCurrencies")
    @Mapping(target = "webhookUrl", source = "request.webhookUrl")
    @Mapping(
            target = "declineRateAlertThresholdBps",
            source = "request.declineRateAlertThresholdBps")
    @Mapping(
            target = "paymentExpirationSeconds",
            source = "request.paymentExpirationSeconds",
            qualifiedByName = "resolvePaymentExpirationSeconds")
    @Mapping(
            target = "refundExpirationSeconds",
            source = "request.refundExpirationSeconds",
            qualifiedByName = "resolveRefundExpirationSeconds")
    UpsertMerchantConfigCommand toCommand(String merchantId, UpsertMerchantConfigRequest request);

    @Mapping(target = "status", expression = "java(config.status().name())")
    MerchantConfigResponse toResponse(MerchantConfig config);

    @Named("resolvePaymentExpirationSeconds")
    static int resolvePaymentExpirationSeconds(Integer requested) {
        return requested == null ? MerchantConfig.DEFAULT_PAYMENT_EXPIRATION_SECONDS : requested;
    }

    @Named("resolveRefundExpirationSeconds")
    static int resolveRefundExpirationSeconds(Integer requested) {
        return requested == null ? MerchantConfig.DEFAULT_REFUND_EXPIRATION_SECONDS : requested;
    }
}
