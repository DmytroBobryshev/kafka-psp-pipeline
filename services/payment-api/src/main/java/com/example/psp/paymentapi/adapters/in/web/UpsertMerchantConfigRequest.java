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

/**
 * Wire contract for {@code PUT /api/merchants/{merchantId}/config} (M10). Records for DTOs, per
 * PLAN.md.
 *
 * <p>{@code merchantId} is deliberately absent: it is the path variable, so the URL is the single
 * place the identity is stated and a body/path mismatch is impossible by construction.
 *
 * <p>The verb is {@code PUT}, not {@code PATCH}, and every field except {@code webhookUrl} and
 * (M22) {@code paymentExpirationSeconds} is required - because a compacted topic stores
 * whole-state snapshots (see {@link com.example.psp.paymentapi.domain.model.MerchantConfig}). A
 * {@code PATCH} would have to read the current value to merge into, and the only place that value
 * lives is the topic itself. {@code paymentExpirationSeconds} is the one other genuinely optional
 * field: {@code null} means "use the default" (900s, {@code adapters.in.web.
 * MerchantConfigWebMapper} resolves it before the command reaches the domain constructor) rather
 * than every caller having to know and repeat that default explicitly.
 */
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
        // M22: null -> 900 (MerchantConfigWebMapper resolves the default); @Min/@Max are
        // skipped by Bean Validation when the value is null, so a caller that omits this field
        // entirely never trips these bounds - only an explicit out-of-range value does, with the
        // usual 400 problem+json (common-web's GlobalExceptionHandler).
        @Min(value = 30, message = "paymentExpirationSeconds must be at least 30")
                @Max(value = 86_400, message = "paymentExpirationSeconds must not exceed 86400 (24h)")
                Integer paymentExpirationSeconds) {
}
