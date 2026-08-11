package com.example.psp.paymentapi.adapters.in.web;

import com.example.psp.paymentapi.domain.model.MerchantStatus;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

/**
 * Wire contract for {@code PUT /api/merchants/{merchantId}/config} (M10). Records for DTOs, per
 * PLAN.md.
 *
 * <p>{@code merchantId} is deliberately absent: it is the path variable, so the URL is the single
 * place the identity is stated and a body/path mismatch is impossible by construction.
 *
 * <p>The verb is {@code PUT}, not {@code PATCH}, and every field except {@code webhookUrl} is
 * required - because a compacted topic stores whole-state snapshots (see
 * {@link com.example.psp.paymentapi.domain.model.MerchantConfig}). A {@code PATCH} would have to
 * read the current value to merge into, and the only place that value lives is the topic itself.
 */
public record UpsertMerchantConfigRequest(
        @NotBlank(message = "displayName must not be blank") String displayName,
        @NotNull(message = "status must not be null") MerchantStatus status,
        @NotBlank(message = "payoutCurrency must not be blank")
                @Pattern(
                        regexp = "[A-Z]{3}",
                        message = "payoutCurrency must be an ISO-4217 3-letter code")
                String payoutCurrency,
        String webhookUrl,
        @Min(value = 0, message = "declineRateAlertThresholdBps must not be negative")
                @Max(value = 10_000, message = "declineRateAlertThresholdBps must not exceed 10000 (100%)")
                int declineRateAlertThresholdBps) {
}
