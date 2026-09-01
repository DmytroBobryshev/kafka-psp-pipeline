package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import java.util.UUID;

/**
 * Outbound port for publishing {@code payments.payment-status-changed.v1}. Implemented by
 * {@code adapters.out.kafka.KafkaPaymentStatusPublisher} - the domain never imports
 * {@code org.apache.kafka} or Spring Kafka directly (ADR-0007).
 *
 * <p>Callers (see {@code application.ProcessPaymentRequestUseCase}) MUST NOT invoke this for a
 * {@link com.example.psp.pspconnector.domain.model.ProviderOutcome#TIMEOUT} attempt - ADR-0006
 * category A failures are never published as domain events, only categories B (declined/approved)
 * are. The adapter enforces this with a defensive check as a second line of defence.
 */
public interface PaymentStatusPublisher {

    void publishStatusChanged(PaymentAttempt attempt);

    /** Non-terminal PENDING, emitted before the provider call; a fresh eventId every time. */
    void publishPending(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID causationEventId,
            String traceId,
            String correlationId);

    /**
     * Non-terminal IPN_RECEIVED (stage 3 of the panel's trail), emitted right after {@link
     * com.example.psp.pspconnector.domain.port.PaymentProviderPort#authorize} returns - a fresh
     * eventId every time, same shape as {@link #publishPending}. {@code providerReference} is the
     * provider's own event id for this attempt ({@link
     * com.example.psp.pspconnector.domain.model.ProviderResult#providerEventId()}).
     */
    void publishIpnReceived(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId);

    /**
     * Non-terminal VERIFIED (stage 4, "call to psp to get status" resolved as genuinely new work),
     * emitted once both M5 dedup levels have cleared and the attempt row is durably recorded - a
     * fresh eventId every time, same shape as {@link #publishPending}.
     */
    void publishVerified(
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId);
}
