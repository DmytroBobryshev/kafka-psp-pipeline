package com.example.psp.webhooknotifier.application;

import com.example.psp.webhooknotifier.domain.model.WebhookDelivery;
import com.example.psp.webhooknotifier.domain.port.DeliveryAttemptLogRepository;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class ListWebhookDeliveriesUseCase {

    private static final int MIN_LIMIT = 1;

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
