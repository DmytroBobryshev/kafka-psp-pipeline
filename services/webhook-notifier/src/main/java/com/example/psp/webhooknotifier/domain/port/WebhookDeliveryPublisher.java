package com.example.psp.webhooknotifier.domain.port;

import com.example.psp.webhooknotifier.domain.model.RetryEnvelope;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Outbound port for publishing a delivery command onto ANY topic in the retry chain - the base
 * topic (the planner's first publish), a retry tier (a non-blocking retry hop), or the DLQ.
 * Implemented by {@code adapters.out.kafka.KafkaWebhookDeliveryPublisher}.
 *
 * <p>One port, two methods, because the two publish sites have genuinely different non-blocking
 * requirements:
 *
 * <ul>
 *   <li>{@link #publishNow} - the planner's first publish, and any DLQ publish (there is no delay
 *       before giving up). Sends immediately.
 *   <li>{@link #publishDelayed} - a retry hop. The delay MUST be honoured without blocking the
 *       calling thread (no {@code Thread.sleep} - see the adapter's javadoc for exactly why and
 *       the M4 cross-reference), so this method returns as soon as the delayed send is
 *       SCHEDULED, not after it fires; the returned future completes only once the send has
 *       actually happened.
 * </ul>
 *
 * <p>Both methods return a future rather than {@code void} because
 * {@code application.ExecuteWebhookDeliveryUseCase} must not acknowledge the Kafka record it is
 * currently processing until the handoff to the next hop (or the DLQ) has actually succeeded -
 * see that class's javadoc for why holding the ack open across a scheduled delay is the correct,
 * not merely convenient, choice here.
 */
public interface WebhookDeliveryPublisher {

    /** Publishes immediately to {@code topic}, keyed by {@code command.merchantId()} (ADR-0003). */
    CompletableFuture<Void> publishNow(String topic, WebhookDeliveryCommand command, RetryEnvelope envelope);

    /**
     * Schedules a publish to {@code topic} after {@code delay} elapses, without blocking the
     * calling thread. The returned future completes when the scheduled send completes (or fails).
     */
    CompletableFuture<Void> publishDelayed(
            String topic, WebhookDeliveryCommand command, RetryEnvelope envelope, Duration delay);
}
