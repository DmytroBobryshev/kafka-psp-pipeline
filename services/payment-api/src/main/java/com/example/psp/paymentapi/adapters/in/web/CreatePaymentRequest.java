package com.example.psp.paymentapi.adapters.in.web;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.math.BigDecimal;

/**
 * Wire contract for {@code POST /api/payments}. Records for DTOs, per PLAN.md.
 *
 * <p>Bean validation (M3) runs at the web boundary, before anything touches the domain or
 * Postgres - {@link PaymentController#create} is annotated {@code @Valid}. {@link
 * com.example.psp.paymentapi.domain.model.Money}'s own constructor still enforces the same
 * non-negativity invariant, so validation is redundant-by-design at that boundary: a bug that
 * bypasses this DTO can never construct an invalid {@code Money}.
 */
public record CreatePaymentRequest(
        @NotBlank(message = "merchantId must not be blank") String merchantId,
        @NotNull(message = "amount must not be null")
                @DecimalMin(value = "0.0", inclusive = true, message = "amount must not be negative")
                BigDecimal amount,
        @NotBlank(message = "currency must not be blank")
                @Pattern(regexp = "[A-Z]{3}", message = "currency must be an ISO-4217 3-letter code")
                String currency) {
}
