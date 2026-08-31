package com.example.psp.webhooknotifier.domain.port;

import com.example.psp.webhooknotifier.domain.model.DeliveryAttempt;
import com.example.psp.webhooknotifier.domain.model.WebhookDelivery;
import java.util.List;
import java.util.UUID;

/**
 * Outbound port for the MongoDB delivery-attempt log (M8 requirement #6). Implemented by
 * {@code adapters.out.persistence.MongoDeliveryAttemptLogRepository}. One document per call to
 * {@link #record}, no update-in-place - the collection is an append-only attempt history.
 *
 * <p>M19 adds {@link #search}, the read side backing {@code GET /api/webhooks/deliveries}
 * (deliveries visibility) - a query over the SAME append-only collection, grouped into the
 * "future read-model" this class's original javadoc already anticipated, not a second collection.
 */
public interface DeliveryAttemptLogRepository {

    /** Persists one attempt. Called for every outcome - success included - never skipped. */
    void record(DeliveryAttempt attempt);

    /**
     * M19: every {@link WebhookDelivery} matching the given (optional) filters, newest first by
     * {@link WebhookDelivery#lastAttemptAt()}, capped at {@code limit} rows. Each row aggregates
     * every {@link DeliveryAttempt} sharing the same {@code causationEventId} into one logical
     * delivery - see {@link WebhookDelivery}'s javadoc.
     *
     * @param paymentId  filter, or {@code null} to match every payment.
     * @param refundId   filter, or {@code null} to match every refund (and every payment-only
     *                   notification).
     * @param merchantId filter, or {@code null} to match every merchant.
     * @param limit      caller's requested cap - already clamped by
     *                   {@code application.ListWebhookDeliveriesUseCase}.
     */
    List<WebhookDelivery> search(UUID paymentId, UUID refundId, String merchantId, int limit);
}
