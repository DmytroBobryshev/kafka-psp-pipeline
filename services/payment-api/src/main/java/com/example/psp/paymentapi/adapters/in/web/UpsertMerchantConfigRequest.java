package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.domain.model.MerchantStatus;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;

public record UpsertMerchantConfigRequest(
        @NotBlank(message = "displayName must not be blank") String displayName,
        @NotNull(message = "status must not be null") MerchantStatus status,
        @NotBlank(message = "payoutCurrency must not be blank")
                @Pattern(
                        regexp = "[A-Z]{3}",
                        message = "payoutCurrency must be an ISO-4217 3-letter code")
                String payoutCurrency,
        @NotEmpty(message = "allowedCurrencies must not be empty")
                @Size(min = 1, max = 3, message = "allowedCurrencies must contain 1 to 3 currency codes")
                List<
                        @Pattern(
                                        regexp = "[A-Z]{3}",
                                        message = "each allowedCurrencies entry must be an ISO-4217 3-letter code")
                                String>
                        allowedCurrencies,
        String webhookUrl,
        @Min(value = 0, message = "declineRateAlertThresholdBps must not be negative")
                @Max(value = 10_000, message = "declineRateAlertThresholdBps must not exceed 10000 (100%)")
                int declineRateAlertThresholdBps,
        @Min(value = 30, message = "paymentExpirationSeconds must be at least 30")
                @Max(value = 86_400, message = "paymentExpirationSeconds must not exceed 86400 (24h)")
                Integer paymentExpirationSeconds,
        @Min(value = 30, message = "refundExpirationSeconds must be at least 30")
                @Max(value = 86_400, message = "refundExpirationSeconds must not exceed 86400 (24h)")
                Integer refundExpirationSeconds) {
}
