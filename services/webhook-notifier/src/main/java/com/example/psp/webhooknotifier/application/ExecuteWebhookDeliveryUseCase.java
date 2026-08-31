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

/**
 * The "executor" half of webhook-notifier (topic-map's {@code webhook-notifier.executor.v1}
 * consumer group, subscribed to the base delivery topic plus all three retry tiers): attempt one
 * HTTP callback, log the attempt, and route the outcome per ADR-0006.
 *
 * <h2>Why this returns a future instead of {@code void}</h2>
 *
 * <p>{@code adapters.in.kafka.WebhookDeliveryExecutorListener} does not acknowledge the Kafka
 * record until the future returned here completes successfully. That is deliberate, not an
 * oversight: for a {@link DeliveryOutcome#RETRYABLE_FAILURE} that still has a tier left in the
 * chain, "handled" does not mean "we attempted delivery" - it means "responsibility for this
 * delivery has been durably handed off to the next tier". If this process crashed between acking
 * the current record and the scheduled hop to the next tier actually firing, the retry would be
 * silently lost - the exact kind of gap M5's idempotent-consumer module exists to close
 * elsewhere in this system. Holding the ack open costs nothing here: Spring Kafka does not
 * require offsets to be acknowledged in order, so other records - on this partition or any
 * other - continue to be polled and processed while one record's handoff is still pending; only
 * THIS record's own offset commit is deferred. If the process crashes before the handoff
 * completes, the record is simply redelivered on restart and this method runs again - correct
 * at-least-once behaviour, not a bug.
 *
 * <h2>What "non-blocking" means here</h2>
 *
 * <p>Nothing in this class sleeps. The one truly time-consuming step -
 * {@link MerchantWebhookClient#deliver} - is a single synchronous HTTP call bounded by the
 * client's own connect/read timeouts ({@code webhook-notifier.merchant-client.*}), not an
 * open-ended wait; it is exactly as long as any other Kafka listener's normal processing work
 * (compare psp-connector's provider call). The RETRY DELAY itself - the 5s/1m/15m between tiers -
 * never happens on this thread at all: {@link WebhookDeliveryPublisher#publishDelayed} schedules
 * the next hop on a separate scheduler thread and returns immediately. See that port's javadoc
 * and {@code config.KafkaConsumerConfig}'s comment on why blocking here would repeat M4's
 * measured rebalance storm.
 */
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

    /**
     * @param command   the delivery payload.
     * @param coordinates where this specific record lives - used only to stamp {@code original-*}
     *                  the first time a delivery fails (see {@link RetryEnvelope#withOriginalIfAbsent}).
     * @param envelope  the retry envelope as read off this record's headers by the listener.
     */
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
                // ADR-0006's outbound-HTTP analogue of category C: a merchant 4xx will not improve
                // by retrying. Straight to the DLQ, skipping the retry chain entirely - never
                // published to .retry.5s, no matter which tier this attempt happened on.
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
