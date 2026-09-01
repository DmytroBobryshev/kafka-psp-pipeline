package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.application.UpsertMerchantConfigCommand;
import com.example.psp.paymentapi.domain.model.MerchantConfig;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
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
    @Mapping(
            target = "paymentExpirationSeconds",
            source = "request.paymentExpirationSeconds",
            qualifiedByName = "resolvePaymentExpirationSeconds")
    @Mapping(
            target = "refundExpirationSeconds",
            source = "request.refundExpirationSeconds",
            qualifiedByName = "resolveRefundExpirationSeconds")
    UpsertMerchantConfigCommand toCommand(String merchantId, UpsertMerchantConfigRequest request);

    /** Enum {@code ->} String is MapStruct's built-in conversion; named explicitly for grep-ability. */
    @Mapping(target = "status", expression = "java(config.status().name())")
    MerchantConfigResponse toResponse(MerchantConfig config);

    /**
     * M22: {@code null} (the field absent from the request body) resolves to
     * {@link MerchantConfig#DEFAULT_PAYMENT_EXPIRATION_SECONDS} here, once, so every downstream
     * layer (command, domain, Avro, projection) always sees a concrete {@code int} - the same
     * "resolve the optional field at the boundary" shape as {@code webhookUrl} staying
     * {@code null} straight through instead (webhookUrl has no default to resolve TO; this field
     * does).
     */
    @Named("resolvePaymentExpirationSeconds")
    static int resolvePaymentExpirationSeconds(Integer requested) {
        return requested == null ? MerchantConfig.DEFAULT_PAYMENT_EXPIRATION_SECONDS : requested;
    }

    /** M24: the refund-path mirror of {@link #resolvePaymentExpirationSeconds} - same null -> default resolution. */
    @Named("resolveRefundExpirationSeconds")
    static int resolveRefundExpirationSeconds(Integer requested) {
        return requested == null ? MerchantConfig.DEFAULT_REFUND_EXPIRATION_SECONDS : requested;
    }
}
