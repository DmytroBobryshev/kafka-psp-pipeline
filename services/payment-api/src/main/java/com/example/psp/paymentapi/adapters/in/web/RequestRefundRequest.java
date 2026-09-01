package com.example.psp.paymentapi.adapters.in.web;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.math.BigDecimal;

public record RequestRefundRequest(
        @NotNull(message = "amount must not be null")
                @DecimalMin(value = "0.0", inclusive = false, message = "amount must be positive")
                BigDecimal amount,
        @NotNull(message = "currency must not be null")
                @Pattern(regexp = "[A-Z]{3}", message = "currency must be an ISO-4217 3-letter code")
                String currency,
        String reason) {
}
