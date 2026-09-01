package com.example.psp.paymentapi.adapters.in.web;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.math.BigDecimal;

public record CreatePaymentRequest(
        @NotBlank(message = "merchantId must not be blank") String merchantId,
        @NotNull(message = "amount must not be null")
                @DecimalMin(value = "0.0", inclusive = true, message = "amount must not be negative")
                BigDecimal amount,
        @NotBlank(message = "currency must not be blank")
                @Pattern(regexp = "[A-Z]{3}", message = "currency must be an ISO-4217 3-letter code")
                String currency) {
}
