package com.example.psp.webhooknotifier.domain.port;

import com.example.psp.webhooknotifier.domain.model.RetryEnvelope;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface WebhookDeliveryPublisher {

    CompletableFuture<Void> publishNow(String topic, WebhookDeliveryCommand command, RetryEnvelope envelope);

    CompletableFuture<Void> publishDelayed(
            String topic, WebhookDeliveryCommand command, RetryEnvelope envelope, Duration delay);
}
