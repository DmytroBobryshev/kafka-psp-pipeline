package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Payment;
import java.time.Instant;
import java.util.UUID;

/**
 * Outbound port for {@code adapters.in.scheduler.PaymentExpirationScheduler} (M22): publishes an
 * {@code EXPIRED} record to {@code payments.payment-status-changed.v1} - the SAME topic
 * psp-connector already produces every other status change to. This is deliberately the only
 * write this scheduler pass performs; {@code payments.status} itself is never touched from here
 * (see {@code application.ExpirePaymentsUseCase}'s javadoc) - it moves only when THIS service's
 * own {@code adapters.in.kafka.PaymentStatusChangedListener} consumes the very record this port
 * publishes, the identical "publish an event, let your own consumer apply it" shape every other
 * status change in this system already goes through.
 *
 * <p>{@code eventId} is caller-supplied, not minted here - {@code application.ExpirePaymentsUseCase}
 * derives it deterministically from {@code paymentId} (see that class's javadoc), so a scheduler
 * tick that republishes a candidate it already published on an earlier tick (still CREATED/PENDING
 * because the listener has not yet caught up, or the record was lost) reuses the exact same id,
 * and {@code payment_status_history}'s {@code UNIQUE(event_id)} constraint (V9) is what turns that
 * republish into a harmless no-op downstream rather than a duplicate history row.
 */
public interface PaymentExpirationEventPublisher {

    /**
     * @param payment   the candidate payment - {@link Payment#getMerchantId()}/{@link
     *                  Payment#getAmount()} become the event's {@code merchantId}/{@code
     *                  amount}/{@code currency}; {@link Payment#getStatus()} is NOT read (the
     *                  event's own status is always the literal {@code "EXPIRED"}).
     * @param eventId   the deterministic id derived from {@code payment.getId()} - see this
     *                  interface's javadoc.
     * @param occurredAt the domain event time to stamp on the envelope (the scheduler's own
     *                  {@code Instant.now(clock)} - see {@code application.ExpirePaymentsUseCase}).
     */
    void publishExpired(Payment payment, UUID eventId, Instant occurredAt);
}
