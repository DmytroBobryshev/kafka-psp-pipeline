package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.Money;

/**
 * Application-layer input model for {@link CreatePaymentUseCase}. Deliberately separate from the
 * web DTO in {@code adapters/in/web} - the web layer maps its request onto this command, so the
 * use case never depends on a web contract, and this command could equally be driven by a future
 * {@code adapters/in/kafka} listener.
 */
public record CreatePaymentCommand(String merchantId, Money amount) {
}
