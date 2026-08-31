package com.example.psp.webhooknotifier.application;

import com.example.psp.webhooknotifier.domain.model.WebhookDelivery;
import com.example.psp.webhooknotifier.domain.port.DeliveryAttemptLogRepository;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * M19's deliveries-visibility use case: backs {@code GET /api/webhooks/deliveries}. A single
 * read against {@link DeliveryAttemptLogRepository#search}, plus the one thing that belongs at
 * this layer rather than the web adapter or the Mongo adapter - clamping the caller's requested
 * {@code limit} to a sane ceiling, the same "clamp, don't reject" convention
 * {@code payment-api}'s {@code PaymentQueryController} uses for its own page size.
 */
@Service
public class ListWebhookDeliveriesUseCase {

    /** {@code limit} clamp floor. */
    private static final int MIN_LIMIT = 1;

    /**
     * {@code limit} clamp ceiling - one request should not be able to force an unbounded
     * aggregation over the whole {@code delivery_attempts} collection. Mirrors
     * {@code webhook-notifier.dlq-replay.max-batch-size}'s existing role as a hard ceiling on a
     * caller-supplied batch size in this same service.
     */
    private static final int MAX_LIMIT = 200;

    private final DeliveryAttemptLogRepository repository;

    public ListWebhookDeliveriesUseCase(DeliveryAttemptLogRepository repository) {
        this.repository = repository;
    }

    public List<WebhookDelivery> execute(UUID paymentId, UUID refundId, String merchantId, int limit) {
        int clampedLimit = Math.min(Math.max(limit, MIN_LIMIT), MAX_LIMIT);
        return repository.search(paymentId, refundId, merchantId, clampedLimit);
    }
}
