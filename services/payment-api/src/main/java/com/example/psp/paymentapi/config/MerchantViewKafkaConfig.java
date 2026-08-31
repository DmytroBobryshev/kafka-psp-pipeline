package com.example.psp.paymentapi.config;

import com.example.psp.common.events.avro.MerchantConfigChanged;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.observation.ObservationRegistry;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Consumer wiring for {@code adapters.in.kafka.MerchantConfigChangedListener} - group
 * {@code payment-api.merchant-view.v1}. Same Avro {@code ConsumerFactory} shape as
 * {@code PaymentStatusViewKafkaConfig} ({@code ErrorHandlingDeserializer} wrapping
 * {@link KafkaAvroDeserializer}, {@code specific.avro.reader=true}), manual-immediate acks, and a
 * zero-retry {@link DefaultErrorHandler} with no recoverer - this listener carries no DLQ for the
 * same reason {@code PaymentStatusChangedListener} does not (a derived, lossy read-model
 * projection, ADR-0006).
 *
 * <p>{@code auto.offset.reset=earliest}, explicitly overridden here rather than inherited: this
 * topic is compacted and IS the merchant aggregate's only durable state, so a fresh consumer
 * group must replay it from the start to reconstruct every merchant that existed before this
 * listener was ever deployed - starting from this service's otherwise-default {@code latest}
 * would silently miss all of them.
 */
@Configuration
public class MerchantViewKafkaConfig {

    @Bean
    public ConsumerFactory<String, MerchantConfigChanged> merchantViewConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl,
            @Value("${payment-api.kafka.merchant-view-group-id}") String groupId) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ADR-0006 category C - "MUST be configured on every consumer factory".
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MerchantConfigChanged>
            merchantViewKafkaListenerContainerFactory(
                    ConsumerFactory<String, MerchantConfigChanged> merchantViewConsumerFactory,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, MerchantConfigChanged> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(merchantViewConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);

        // No DLQ for this topic (see MerchantConfigChangedListener's javadoc) - zero retries, log
        // and skip via the default (no-recoverer) DefaultErrorHandler.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }
}
