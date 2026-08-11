package com.example.psp.webhooknotifier.domain.port;

import com.example.psp.webhooknotifier.domain.model.DeliveryResult;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;

/**
 * Outbound port for the actual HTTP callback to a merchant. Implemented by
 * {@code adapters.out.http.RestClientMerchantWebhookClient}, which calls a REAL HTTP endpoint -
 * by default the in-process {@code adapters.in.web.SimulatedMerchantController} running in this
 * same service, per M8's brief ("An in-process controller the service calls over real HTTP is
 * fine and preferable to mocking, so the failure is genuinely an HTTP failure"). A production
 * implementation of this port would call an actual merchant's registered webhook URL instead,
 * with zero change to {@code application/} - same carve-out psp-connector's
 * {@code PaymentProviderPort} makes for outbound calls (ADR-0004).
 */
public interface MerchantWebhookClient {

    /**
     * Attempts one HTTP delivery. Never throws for a classifiable outcome (success, merchant
     * 4xx/5xx, timeout) - those are all returned as a {@link DeliveryResult} for the use case to
     * act on. An exception escaping this method is, by construction, something this adapter could
     * not classify (ADR-0006 category D) and is left to propagate to the Kafka container's error
     * handler - see {@code config.KafkaConsumerConfig}.
     */
    DeliveryResult deliver(WebhookDeliveryCommand command);
}
