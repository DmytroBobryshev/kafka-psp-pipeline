package com.example.psp.webhooknotifier.config;

import com.example.psp.webhooknotifier.adapters.in.kafka.PaymentStatusChangedEvent;
import com.example.psp.webhooknotifier.adapters.in.kafka.WebhookDeliveryRequestedEvent;
import com.example.psp.webhooknotifier.adapters.out.kafka.WebhookDeliveryRequested;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Consumer wiring for both of webhook-notifier's consumer groups (docs/diagrams/topic-map.md:
 * {@code webhook-notifier.planner.v1} and {@code webhook-notifier.executor.v1}) plus the
 * dedicated, non-listening reader used by DLQ replay.
 *
 * <h2>The M8 poison-pill flag</h2>
 *
 * <p>{@code webhook-notifier.kafka.deserialization-error-handling-enabled} (default {@code true})
 * gates {@link ErrorHandlingDeserializer} on EVERY consumer factory below. Run with it set to
 * {@code false} - {@code --webhook-notifier.kafka.deserialization-error-handling-enabled=false} -
 * to reproduce the failure M8's "prove it" experiment is built around: publish a record whose
 * bytes are not valid JSON for the target type, and the raw {@link JsonDeserializer} throws
 * straight out of {@code poll()}, before this application's code - listener, error handler,
 * anything - ever runs. The container has no record to hand off and nothing to advance past, so
 * it re-fetches the SAME offset and fails again, forever: one poisoned offset spins one partition
 * at 100% CPU while every later record behind it queues up unprocessed, and the consumer never
 * leaves the group (it is not stuck in application code, so heartbeats keep flowing and no
 * rebalance is even triggered - this is a quieter, more insidious failure than M4's rebalance
 * storm, not a louder one). Flip the property back to {@code true} and the SAME bad record is
 * instead caught by {@link ErrorHandlingDeserializer} before the listener runs, handed to
 * {@link DefaultErrorHandler}, and (on the executor factory) published straight to the DLQ by
 * {@link DeadLetterPublishingRecoverer} - the fix, applied without changing a single byte on the
 * topic. See the README's "Poison pill proof" placeholder for the orchestrator's exact commands.
 *
 * <h2>Why the executor's error handler never retries</h2>
 *
 * <p>Every RETRYABLE outcome (ADR-0006 category A: merchant 5xx, timeout) is caught and routed by
 * {@code application.ExecuteWebhookDeliveryUseCase} itself, inside the listener, via the
 * non-blocking {@code domain.model.RetryChain} - it never becomes a thrown exception the
 * container's error handler sees. What DOES reach {@link DefaultErrorHandler} here is only what
 * that use case could not classify at all: a deserialization failure (category C) or a genuine
 * bug (category D, "unknown = non-retryable" per ADR-0006). Both get exactly {@code FixedBackOff(0, 0)} -
 * zero retries - before {@link DeadLetterPublishingRecoverer} publishes straight to the DLQ.
 * Retrying an unclassified exception would silently turn every bug into an infinite loop, which is
 * exactly the mistake ADR-0006 calls out category D to prevent.
 */
@Configuration
public class KafkaConsumerConfig {

    // ============================================================================================
    // Planner: payments.payment-status-changed.v1 -> webhook-notifier.planner.v1
    // ============================================================================================

    @Bean
    public ConsumerFactory<String, PaymentStatusChangedEvent> plannerConsumerFactory(
            KafkaProperties kafkaProperties, WebhookNotifierProperties properties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.kafka().plannerGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configureDeserializers(
                props, PaymentStatusChangedEvent.class, properties.kafka().deserializationErrorHandlingEnabled());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChangedEvent>
            plannerKafkaListenerContainerFactory(
                    @Qualifier("plannerConsumerFactory") ConsumerFactory<String, PaymentStatusChangedEvent> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChangedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // No DLQ for this topic (see PaymentStatusChangedListener's javadoc) - zero retries, log
        // and skip via the default (no-recoverer) DefaultErrorHandler.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }

    // ============================================================================================
    // Executor: webhooks.webhook-delivery-requested.v1 (+3 retry tiers) -> webhook-notifier.executor.v1
    // ============================================================================================

    @Bean
    public ConsumerFactory<String, WebhookDeliveryRequestedEvent> executorConsumerFactory(
            KafkaProperties kafkaProperties, WebhookNotifierProperties properties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.kafka().executorGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configureDeserializers(
                props,
                WebhookDeliveryRequestedEvent.class,
                properties.kafka().deserializationErrorHandlingEnabled());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WebhookDeliveryRequestedEvent>
            executorKafkaListenerContainerFactory(
                    @Qualifier("executorConsumerFactory")
                            ConsumerFactory<String, WebhookDeliveryRequestedEvent> consumerFactory,
                    KafkaTemplate<String, Object> kafkaTemplate,
                    WebhookNotifierProperties properties) {
        ConcurrentKafkaListenerContainerFactory<String, WebhookDeliveryRequestedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // The listener itself calls Acknowledgment.acknowledge() only once ExecuteWebhookDeliveryUseCase's
        // returned future completes - see WebhookDeliveryExecutorListener's javadoc.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        String dlqTopic = properties.kafka().dlqTopic();
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate, (record, exception) -> new TopicPartition(dlqTopic, -1));
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0L)));
        return factory;
    }

    // ============================================================================================
    // DLQ replay reader (adapters.out.kafka.KafkaDlqReader) - NOT a @KafkaListener container; a
    // ConsumerFactory a plain Consumer is created from, on demand, per REST call.
    // ============================================================================================

    @Bean
    public ConsumerFactory<String, Object> dlqReplayConsumerFactory(
            KafkaProperties kafkaProperties, WebhookNotifierProperties properties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.dlqReplay().consumerGroup());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // The hard ceiling on one poll(), independent of what a caller requests - see
        // KafkaDlqReader's javadoc "The guard, mechanically".
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.dlqReplay().maxBatchSize());
        configureDeserializers(
                props, WebhookDeliveryRequested.class, properties.kafka().deserializationErrorHandlingEnabled());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private static void configureDeserializers(
            Map<String, Object> props, Class<?> targetType, boolean errorHandlingEnabled) {
        if (errorHandlingEnabled) {
            // ADR-0006 category C - "MUST be configured on every consumer factory". See this
            // class's javadoc for what breaks when this branch is not taken.
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
            props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        } else {
            // THE poison-pill drill's "before" state - see this class's javadoc.
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        }
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, targetType.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.psp.*");
    }
}
