package com.example.psp.webhooknotifier.config;

import com.example.psp.common.events.avro.MerchantConfigChanged;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.common.events.avro.RefundStatusChanged;
import com.example.psp.common.events.avro.WebhookDeliveryRequested;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.observation.ObservationRegistry;
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

@Configuration
public class KafkaConsumerConfig {

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
                    @Qualifier("plannerConsumerFactory") ConsumerFactory<String, PaymentStatusChanged> consumerFactory,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChanged> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);

        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }

    @Bean
    public ConsumerFactory<String, RefundCompleted> refundCompletedConsumerFactory(
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
    public ConcurrentKafkaListenerContainerFactory<String, RefundCompleted>
            refundCompletedKafkaListenerContainerFactory(
                    @Qualifier("refundCompletedConsumerFactory") ConsumerFactory<String, RefundCompleted> consumerFactory,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, RefundCompleted> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }

    @Bean
    public ConsumerFactory<String, RefundFailed> refundFailedConsumerFactory(
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
    public ConcurrentKafkaListenerContainerFactory<String, RefundFailed>
            refundFailedKafkaListenerContainerFactory(
                    @Qualifier("refundFailedConsumerFactory") ConsumerFactory<String, RefundFailed> consumerFactory,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, RefundFailed> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }

    @Bean
    public ConsumerFactory<String, RefundStatusChanged> refundStatusChangedConsumerFactory(
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
    public ConcurrentKafkaListenerContainerFactory<String, RefundStatusChanged>
            refundStatusChangedKafkaListenerContainerFactory(
                    @Qualifier("refundStatusChangedConsumerFactory")
                            ConsumerFactory<String, RefundStatusChanged> consumerFactory,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, RefundStatusChanged> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }

    @Bean
    public ConsumerFactory<String, MerchantConfigChanged> merchantConfigChangedConsumerFactory(
            KafkaProperties kafkaProperties,
            WebhookNotifierProperties properties,
            @Value("${webhook-notifier.schema-registry.url}") String schemaRegistryUrl) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.kafka().merchantViewGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configureAvroDeserializers(
                props, schemaRegistryUrl, properties.kafka().deserializationErrorHandlingEnabled());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MerchantConfigChanged>
            merchantViewKafkaListenerContainerFactory(
                    @Qualifier("merchantConfigChangedConsumerFactory")
                            ConsumerFactory<String, MerchantConfigChanged> consumerFactory,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, MerchantConfigChanged> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }

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
                    WebhookNotifierProperties properties,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, WebhookDeliveryRequested> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);

        String dlqTopic = properties.kafka().dlqTopic();
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        dlqKafkaTemplate, (record, exception) -> new TopicPartition(dlqTopic, -1));
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0L)));
        return factory;
    }

    @Bean
    public ConsumerFactory<String, Object> dlqReplayConsumerFactory(
            KafkaProperties kafkaProperties, WebhookNotifierProperties properties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.dlqReplay().consumerGroup());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
