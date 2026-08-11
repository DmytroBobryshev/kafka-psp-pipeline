package com.example.psp.ledger.domain.port;

import com.example.psp.ledger.domain.model.MerchantBalance;
import com.example.psp.ledger.domain.model.RefundRequest;
import com.example.psp.ledger.domain.model.RefundSagaState;
import java.util.UUID;

/**
 * Outbound port for the ledger's refund-saga publications (M11): {@code
 * refunds.funds-reserved.v1}, the insufficient-balance branch of {@code refunds.refund-failed.v1},
 * and the compensating {@code refunds.reservation-released.v1}. Implemented by
 * {@code adapters.out.kafka.KafkaRefundEventPublisher} - the domain never imports Kafka or Avro
 * directly (ADR-0007). Every signature below uses {@code domain/} types only, same rule
 * {@link LedgerEntryPublisher} and {@code psp-connector}'s {@code PaymentStatusPublisher} already
 * follow.
 */
public interface RefundEventPublisher {

    /**
     * Publishes {@code refunds.funds-reserved.v1}, keyed by {@code merchantId} (ADR-0003).
     *
     * @param causationEventId the inbound {@code refunds.refund-requested.v1} event's own
     *                         {@code eventId}.
     */
    void publishFundsReserved(
            RefundRequest request,
            UUID causationEventId,
            String traceId,
            String correlationId,
            MerchantBalance balanceAfter);

    /**
     * Publishes {@code refunds.refund-failed.v1} with {@code reason = "INSUFFICIENT_BALANCE"} -
     * the ledger's own decision point (ADR-0006 category B, not an error), produced instead of
     * {@link #publishFundsReserved} for this inbound event.
     *
     * @param causationEventId the inbound {@code refunds.refund-requested.v1} event's own
     *                         {@code eventId}.
     */
    void publishRefundFailedInsufficientBalance(
            RefundRequest request, UUID causationEventId, String traceId, String correlationId);

    /**
     * Publishes {@code refunds.reservation-released.v1} - the compensating event. Two distinct
     * callers, both routed through this one method:
     *
     * <ul>
     *   <li>compensation (a {@code refunds.refund-failed.v1} arrived while RESERVED) -
     *       {@code causationEventId} is that inbound event's own {@code eventId};
     *   <li>the TTL sweeper - {@code causationEventId} is {@code null}; this publish is a root
     *       cause in its own right, not a reaction to any inbound Kafka record.
     * </ul>
     *
     * @param state          the saga state as it stood immediately before this release (its amount
     *                       is what gets restored and what the event reports).
     * @param reason         {@code "COMPENSATION"} or {@code "TIMEOUT"}.
     * @param causationEventId nullable - see above.
     * @param traceId        propagated when {@code causationEventId != null}; a fresh id for the
     *                       TTL path.
     * @param correlationId  propagated when {@code causationEventId != null}; a fresh id for the
     *                       TTL path.
     */
    void publishReservationReleased(
            RefundSagaState state, String reason, UUID causationEventId, String traceId, String correlationId);
}
