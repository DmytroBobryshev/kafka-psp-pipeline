package com.example.psp.paymentapi.domain.model;

/**
 * Lifecycle states of a {@link Payment}. The real state machine (and the transitions
 * {@code psp-connector} drives via {@code payments.status-changed}) arrives in M3/M4; M1 only
 * needs the initial state to exist.
 *
 * <p>{@code EXPIRED} (M22): the terminal state payment-api's own expiration scheduler
 * ({@code adapters.in.scheduler.PaymentExpirationScheduler}) applies to a payment still
 * {@code CREATED}/{@code PENDING} past the owning merchant's configured
 * {@code paymentExpirationSeconds} window - see
 * {@code domain.port.PaymentRepository#applyExpiredStatus} for the conditional guard that keeps
 * this from ever downgrading an already-resolved payment.
 */
public enum PaymentStatus {
    CREATED,
    PENDING,
    SUCCEEDED,
    FAILED,
    EXPIRED
}
