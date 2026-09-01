package com.example.psp.webhooknotifier.domain.port;

import com.example.psp.webhooknotifier.domain.model.DeliveryAttempt;
import com.example.psp.webhooknotifier.domain.model.WebhookDelivery;
import java.util.List;
import java.util.UUID;

public interface DeliveryAttemptLogRepository {

    void record(DeliveryAttempt attempt);

    List<WebhookDelivery> search(UUID paymentId, UUID refundId, String merchantId, int limit);
}
