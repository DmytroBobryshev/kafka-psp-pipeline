package com.example.psp.pspconnector.domain.model;

/**
 * The raw result of one simulated call to the (simulated) payment provider/acquirer.
 *
 * <p>This is deliberately a three-way outcome, not the two-way {@code SUCCEEDED}/{@code FAILED}
 * vocabulary of the outbound {@code payments.payment-status-changed.v1} event: {@link #TIMEOUT}
 * is a technical failure (ADR-0006 category A, retryable) and is <b>never</b> published as a
 * status event, whereas {@link #APPROVED} and {@link #DECLINED} are both business outcomes
 * (ADR-0006 category B) that always get published. Keeping this enum separate from the event's
 * status vocabulary is what makes that distinction impossible to blur by accident - see
 * {@code adapters.out.kafka.PaymentStatusEventMapper}, the only place the two vocabularies meet.
 */
public enum ProviderOutcome {
    APPROVED,
    DECLINED,
    TIMEOUT
}
