package com.example.psp.pspconnector.domain.port;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.RefundAttempt;
import java.util.UUID;

/**
 * Outbound port for publishing the refund saga's execution outcome (M11) plus its M23 non-terminal
 * trail. Unlike {@link PaymentStatusPublisher} (which always publishes to the same topic, encoding
 * the outcome in a {@code status} field), COMPLETED and DECLINED map to two DIFFERENT topics here -
 * {@code refunds.refund-completed.v1} and {@code refunds.refund-failed.v1} - per the module brief
 * (docs/PLAN.md M11) and ADR-0008's topology. One method, because the caller
 * ({@code application.ExecuteRefundUseCase}) should not have to know which topic a given outcome
 * belongs to - that routing decision lives entirely in the adapter
 * ({@code adapters.out.kafka.KafkaRefundStatusPublisher}).
 *
 * <p>M23 adds the PENDING/IPN_RECEIVED/VERIFIED trail on {@code refunds.refund-status-changed.v1},
 * mirroring {@link PaymentStatusPublisher}'s identical three methods field-for-field except for the
 * extra {@code refundId} - the refund saga's aggregateId, separate from {@code paymentId}.
 */
public interface RefundStatusPublisher {

    void publishOutcome(RefundAttempt attempt);

    /** Non-terminal PENDING, emitted before the provider call; a fresh eventId every time. */
    void publishPending(
            UUID refundId,
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID causationEventId,
            String traceId,
            String correlationId);

    /**
     * Non-terminal IPN_RECEIVED, emitted right after
     * {@link com.example.psp.pspconnector.domain.port.RefundProviderPort#refund} returns - a fresh
     * eventId every time. {@code providerReference} is the provider's own reference for this
     * refund attempt.
     */
    void publishIpnReceived(
            UUID refundId,
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId);

    /**
     * Non-terminal VERIFIED, emitted once the attempt row is durably recorded (M5 level 1 cleared)
     * - a fresh eventId every time.
     */
    void publishVerified(
            UUID refundId,
            UUID paymentId,
            String merchantId,
            Money amount,
            UUID providerReference,
            UUID causationEventId,
            String traceId,
            String correlationId);
}
