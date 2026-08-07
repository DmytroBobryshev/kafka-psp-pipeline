package com.example.psp.paymentapi.domain.model;

/**
 * Lifecycle states of a {@link Payment}. The real state machine (and the transitions
 * {@code psp-connector} drives via {@code payments.status-changed}) arrives in M3/M4; M1 only
 * needs the initial state to exist.
 */
public enum PaymentStatus {
    CREATED,
    PENDING,
    SUCCEEDED,
    FAILED
}
