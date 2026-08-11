package com.example.psp.webhooknotifier.config;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.common.events.avro.WebhookDeliveryRequested;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
 * <h2>M9 Phase 2 - Avro on the planner and executor, JSON on the DLQ replay reader</h2>
 *
 * <p>{@code payments.payment-status-changed.v1} (planner) and
 * {@code webhooks.webhook-delivery-requested.v2} plus its three retry tiers (executor) are now
 * Avro + Schema Registry - {@link #configureAvroDeserializers} wires
 * {@code io.confluent.kafka.serializers.KafkaAvroDeserializer} with
 * {@code specific.avro.reader=true}, the same pattern psp-connector's and ledger's consumer
 * configs already use. {@code webhooks.webhook-delivery-requested.v2.dlq} deliberately stays on
 * {@link #configureJsonDeserializers} (unchanged from M8): see
 * {@code adapters.out.kafka.KafkaWebhookDeliveryPublisher}'s and {@code KafkaProducerConfig}'s
 * javadoc for why the DLQ was never a candidate for Avro in the first place.
 *
 * <h2>The M8 poison-pill flag</h2>
 *
 * <p>{@code webhook-notifier.kafka.deserialization-error-handling-enabled} (default {@code true})
 * still gates {@link ErrorHandlingDeserializer} on EVERY consumer factory below, Avro or JSON
 * alike. Run with it set to {@code false} -
 * {@code --webhook-notifier.kafka.deserialization-error-handling-enabled=false} - to reproduce the
 * failure M8's "prove it" experiment is built around: publish a record whose bytes are not valid
 * for the target format, and the raw delegate deserializer throws straight out of {@code poll()},
 * before this application's code - listener, error handler, anything - ever runs. The container
 * has no record to hand off and nothing to advance past, so it re-fetches the SAME offset and
 * fails again, forever. Flip the property back to {@code true} and the SAME bad record is instead
 * caught by {@link ErrorHandlingDeserializer} before the listener runs, handed to
 * {@link DefaultErrorHandler}, and (on the executor factory) published straight to the DLQ by
 * {@link DeadLetterPublishingRecoverer} - the fix, applied without changing a single byte on the
 * topic.
 *
 * <h2>Why the executor's error handler never retries</h2>
 *
 * <p>Every RETRYABLE outcome (ADR-0006 category A: merchant 5xx, timeout) is caught and routed by
 * {@code application.ExecuteWebhookDeliveryUseCase} itself, inside the listener, via the
 * non-blocking {@code domain.model.RetryChain} - it never becomes a thrown exception the
 * container's error handler sees. What DOES reach {@link DefaultErrorHandler} here is only what
 * that use case could not classify at all: a deserialization failure (category C) or a genuine
 * bug (category D, "unknown = non-retryable" per ADR-0006). Both get exactly {@code FixedBackOff(0, 0)} -
 * zero retries - before {@link DeadLetterPublishingRecoverer} publishes straight to the DLQ, using
 * {@code webhookDeliveryDlqKafkaTemplate} (the byte-tolerant JSON template) - never the Avro one,
 * so a poison pill's raw bytes survive the trip.
 */
@Configuration
public class KafkaConsumerConfig {

    // ============================================================================================
    // Planner: payments.payment-status-changed.v1 -> webhook-notifier.planner.v1
    // ============================================================================================

    @Bean
    public ConsumerFactory<String, PaymentStatusChanged> plannerConsumerFactory(
            KafkaProperties kafkaProperties,
            WebhookNotifierProperties properties,
            @Value("${webhook-notifier.schema-registry.url}") String schemaRegistryUrl) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.kafka().plannerGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configureAvroDeserializers(
                props, schemaRegistryUrl, properties.kafka().deserializationErrorHandlingEnabled());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChanged>
            plannerKafkaListenerContainerFactory(
                    @Qualifier("plannerConsumerFactory") ConsumerFactory<String, PaymentStatusChanged> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChanged> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // No DLQ for this topic (see PaymentStatusChangedListener's javadoc) - zero retries, log
        // and skip via the default (no-recoverer) DefaultErrorHandler.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }

    // ============================================================================================
    // Executor: webhooks.webhook-delivery-requested.v2 (+3 retry tiers) -> webhook-notifier.executor.v1
    // ============================================================================================

    @Bean
    public ConsumerFactory<String, WebhookDeliveryRequested> executorConsumerFactory(
            KafkaProperties kafkaProperties,
            WebhookNotifierProperties properties,
            @Value("${webhook-notifier.schema-registry.url}") String schemaRegistryUrl) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.kafka().executorGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configureAvroDeserializers(
                props, schemaRegistryUrl, properties.kafka().deserializationErrorHandlingEnabled());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WebhookDeliveryRequested>
            executorKafkaListenerContainerFactory(
                    @Qualifier("executorConsumerFactory")
                            ConsumerFactory<String, WebhookDeliveryRequested> consumerFactory,
                    @Qualifier("webhookDeliveryDlqKafkaTemplate") KafkaTemplate<String, Object> dlqKafkaTemplate,
                    WebhookNotifierProperties properties) {
        ConcurrentKafkaListenerContainerFactory<String, WebhookDeliveryRequested> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // The listener itself calls Acknowledgment.acknowledge() only once ExecuteWebhookDeliveryUseCase's
        // returned future completes - see WebhookDeliveryExecutorListener's javadoc.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        String dlqTopic = properties.kafka().dlqTopic();
        // dlqKafkaTemplate, NOT the Avro template - a genuine poison pill's value is raw,
        // undeserializable bytes by definition, and KafkaAvroSerializer cannot republish those
        // (see KafkaProducerConfig's javadoc). Using the byte-tolerant JSON template here is what
        // keeps M8's "Poison pill proof" true unchanged.
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        dlqKafkaTemplate, (record, exception) -> new TopicPartition(dlqTopic, -1));
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0L)));
        return factory;
    }

    // ============================================================================================
    // DLQ replay reader (adapters.out.kafka.KafkaDlqReader) - NOT a @KafkaListener container; a
    // plain Consumer created from this factory, on demand, per REST call. Stays JSON: the DLQ
    // itself was never migrated to Avro (see this class's javadoc).
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
        configureJsonDeserializers(
                props,
                com.example.psp.webhooknotifier.adapters.out.kafka.WebhookDeliveryRequested.class,
                properties.kafka().deserializationErrorHandlingEnabled());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private static void configureAvroDeserializers(
            Map<String, Object> props, String schemaRegistryUrl, boolean errorHandlingEnabled) {
        if (errorHandlingEnabled) {
            // ADR-0006 category C - "MUST be configured on every consumer factory".
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
            props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        } else {
            // THE poison-pill drill's "before" state - see this class's javadoc.
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        }
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // specific.avro.reader=true: hand the listener the generated class, not a schema-less
        // GenericRecord - same requirement as every other M9 Avro consumer in this system.
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    }

    private static void configureJsonDeserializers(
            Map<String, Object> props, Class<?> targetType, boolean errorHandlingEnabled) {
        if (errorHandlingEnabled) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
            props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        } else {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        }
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, targetType.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.psp.*");
    }
}
