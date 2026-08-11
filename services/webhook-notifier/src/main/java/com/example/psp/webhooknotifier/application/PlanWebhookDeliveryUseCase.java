package com.example.psp.webhooknotifier.application;

import com.example.psp.webhooknotifier.domain.model.RetryChain;
import com.example.psp.webhooknotifier.domain.model.RetryEnvelope;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import com.example.psp.webhooknotifier.domain.port.WebhookDeliveryPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The "planner" half of webhook-notifier (topic-map's {@code webhook-notifier.planner.v1}
 * consumer group): for every {@code payments.payment-status-changed.v1} event, publish exactly
 * one delivery command to {@code webhooks.webhook-delivery-requested.v1}.
 *
 * <p>Deliberately trivial - a straight translation, no HTTP, no retry logic, no Mongo. That
 * asymmetry is the point of this module's two-consumer-group split (see
 * docs/diagrams/topic-map.md's "Webhook delivery chain" section): the retry chain hangs off the
 * delivery-command topic, not off the payment-status-changed topic, so a merchant with a
 * permanently broken endpoint retries only ITS OWN delivery commands and never backs up the
 * shared payment-status pipeline that ledger/analytics/realtime-gateway also depend on.
 */
@Service
public class PlanWebhookDeliveryUseCase {

    private static final Logger log = LoggerFactory.getLogger(PlanWebhookDeliveryUseCase.class);

    private final WebhookDeliveryPublisher publisher;
    private final RetryChain retryChain;

    public PlanWebhookDeliveryUseCase(WebhookDeliveryPublisher publisher, RetryChain retryChain) {
        this.publisher = publisher;
        this.retryChain = retryChain;
    }

    public void execute(WebhookDeliveryCommand command) {
        log.info(
                "Planning webhook delivery paymentId={} merchantId={} status={}",
                command.paymentId(),
                command.merchantId(),
                command.status());

        // publishNow, not publishDelayed: this is attempt 1, not a retry hop - there is nothing to
        // wait for yet. RetryEnvelope.initial() carries no original-* / exception-* headers because
        // nothing has failed (or even been attempted) yet.
        publisher.publishNow(retryChain.baseTopic(), command, RetryEnvelope.initial());
    }
}
