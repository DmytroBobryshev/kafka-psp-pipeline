package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Refund;
import java.time.Instant;
import java.util.UUID;

/**
 * Outbound port for {@code adapters.in.scheduler.RefundExpirationScheduler} (M24): publishes an
 * {@code EXPIRED} record to {@code refunds.refund-status-changed.v1} - the SAME topic
 * psp-connector already produces PENDING/IPN_RECEIVED/VERIFIED to, the refund-path mirror of
 * {@link PaymentExpirationEventPublisher}. This is the only write this sweep pass performs;
 * {@code refunds.*} itself is never touched (there is nothing to touch - {@link Refund}'s status
 * is always {@code REQUESTED} and never advances, see that class's javadoc). The published record
 * lands back in {@code refund_status_history} only once this service's OWN, already-existing
 * {@code adapters.in.kafka.RefundStatusChangedListener} consumes it - the identical "publish an
 * event, let your own consumer apply it" shape {@link PaymentExpirationEventPublisher}'s javadoc
 * documents for the payment side.
 *
 * <p>{@code eventId} is caller-supplied, not minted here - {@code application.ExpireRefundsUseCase}
 * derives it deterministically from {@code refundId} (see that class's javadoc), so a sweep tick
 * that republishes a candidate it already published on an earlier tick reuses the exact same id,
 * and {@code refund_status_history}'s {@code UNIQUE(event_id)} constraint (V12) turns that
 * republish into a harmless no-op downstream rather than a duplicate history row.
 */
public interface RefundExpirationEventPublisher {

    /**
     * @param refund    the candidate refund - {@link Refund#getMerchantId()}/{@link
     *                  Refund#getPaymentId()}/{@link Refund#getAmount()} become the event's
     *                  {@code merchantId}/{@code paymentId}/{@code amount}/{@code currency}.
     * @param eventId   the deterministic id derived from {@code refund.getId()} - see this
     *                  interface's javadoc.
     * @param occurredAt the domain event time to stamp on the envelope (the sweep's own
     *                  {@code Instant.now(clock)} - see {@code application.ExpireRefundsUseCase}).
     */
    void publishExpired(Refund refund, UUID eventId, Instant occurredAt);
}
