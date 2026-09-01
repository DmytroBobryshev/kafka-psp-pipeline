package com.example.psp.webhooknotifier.application;

import com.example.psp.webhooknotifier.domain.model.RetryChain;
import com.example.psp.webhooknotifier.domain.model.RetryEnvelope;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import com.example.psp.webhooknotifier.domain.port.WebhookDeliveryPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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

        publisher.publishNow(retryChain.baseTopic(), command, RetryEnvelope.initial());
    }
}
