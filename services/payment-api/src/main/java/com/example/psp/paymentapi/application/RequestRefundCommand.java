package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Money;
import java.util.UUID;

/**
 * Application-layer input model for {@link RequestRefundUseCase}. Deliberately separate from the
 * web DTO in {@code adapters/in/web} - same pattern as {@link CreatePaymentCommand}.
 */
public record RequestRefundCommand(UUID paymentId, Money amount, String reason) {
}
