package com.example.psp.webhooknotifier.application;

import com.example.psp.webhooknotifier.domain.model.DeliveryAttempt;
import com.example.psp.webhooknotifier.domain.model.DeliveryOutcome;
import com.example.psp.webhooknotifier.domain.model.DeliveryResult;
import com.example.psp.webhooknotifier.domain.model.RecordCoordinates;
import com.example.psp.webhooknotifier.domain.model.RetryChain;
import com.example.psp.webhooknotifier.domain.model.RetryEnvelope;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import com.example.psp.webhooknotifier.domain.port.DeliveryAttemptLogRepository;
import com.example.psp.webhooknotifier.domain.port.MerchantWebhookClient;
import com.example.psp.webhooknotifier.domain.port.WebhookDeliveryPublisher;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ExecuteWebhookDeliveryUseCase {

    private static final Logger log = LoggerFactory.getLogger(ExecuteWebhookDeliveryUseCase.class);

    private final MerchantWebhookClient merchantClient;
    private final DeliveryAttemptLogRepository attemptLog;
    private final WebhookDeliveryPublisher publisher;
    private final RetryChain retryChain;

    public ExecuteWebhookDeliveryUseCase(
            MerchantWebhookClient merchantClient,
            DeliveryAttemptLogRepository attemptLog,
            WebhookDeliveryPublisher publisher,
            RetryChain retryChain) {
        this.merchantClient = merchantClient;
        this.attemptLog = attemptLog;
        this.publisher = publisher;
        this.retryChain = retryChain;
    }

    public CompletableFuture<Void> execute(
            WebhookDeliveryCommand command, RecordCoordinates coordinates, RetryEnvelope envelope) {
        DeliveryResult result = merchantClient.deliver(command);

        attemptLog.record(
                new DeliveryAttempt(
                        command.merchantId(),
                        command.paymentId(),
                        command.refundId(),
                        command.eventType(),
                        command.causationEventId(),
                        envelope.attemptCount(),
                        result.outcome(),
                        result.statusCode(),
                        result.errorMessage(),
                        coordinates.topic(),
                        Instant.now()));

        return switch (result.outcome()) {
            case SUCCESS -> {
                log.info(
                        "Webhook delivered paymentId={} merchantId={} attempt={} statusCode={}",
                        command.paymentId(),
                        command.merchantId(),
                        envelope.attemptCount(),
                        result.statusCode());
                yield CompletableFuture.completedFuture(null);
            }
            case NON_RETRYABLE_FAILURE -> {
                log.warn(
                        "Webhook delivery non-retryable paymentId={} merchantId={} attempt={} "
                                + "statusCode={} error={} - routing straight to DLQ",
                        command.paymentId(),
                        command.merchantId(),
                        envelope.attemptCount(),
                        result.statusCode(),
                        result.errorMessage());
                RetryEnvelope failed =
                        envelope
                                .withOriginalIfAbsent(coordinates)
                                .withFailure("merchant-4xx", describeFailure(result));
                yield publisher.publishNow(retryChain.dlqTopic(), command, failed);
            }
            case RETRYABLE_FAILURE -> {
                RetryEnvelope failed =
                        envelope
                                .withOriginalIfAbsent(coordinates)
                                .withFailure("merchant-5xx-or-timeout", describeFailure(result));
                yield retryChain
                        .nextTierAfter(coordinates.topic())
                        .map(
                                tier -> {
                                    log.info(
                                            "Webhook delivery retryable paymentId={} merchantId={} attempt={} "
                                                    + "statusCode={} - scheduling hop to {} after {}",
                                            command.paymentId(),
                                            command.merchantId(),
                                            envelope.attemptCount(),
                                            result.statusCode(),
                                            tier.topic(),
                                            tier.delay());
                                    return publisher.publishDelayed(
                                            tier.topic(), command, failed.nextAttempt(), tier.delay());
                                })
                        .orElseGet(
                                () -> {
                                    // Last tier (.retry.15m) failed again - the chain is exhausted.
                                    log.warn(
                                            "Webhook delivery exhausted retry chain paymentId={} merchantId={} "
                                                    + "attempt={} statusCode={} - routing to DLQ",
                                            command.paymentId(),
                                            command.merchantId(),
                                            envelope.attemptCount(),
                                            result.statusCode());
                                    return publisher.publishNow(retryChain.dlqTopic(), command, failed);
                                });
            }
        };
    }

    private static String describeFailure(DeliveryResult result) {
        return "HTTP " + result.statusCode() + ": " + result.errorMessage();
    }
}
