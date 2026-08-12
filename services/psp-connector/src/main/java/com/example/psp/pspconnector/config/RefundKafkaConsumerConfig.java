package com.example.psp.pspconnector.config;

import com.example.psp.common.events.avro.FundsReserved;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.observation.ObservationRegistry;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

/**
 * M11's consumer wiring for {@code refunds.funds-reserved.v1} - a separate class from M4's
 * {@code KafkaConsumerConfig} on purpose, same reasoning as the ledger's equivalent split: that
 * class and the {@code payments.payment-requested.v1} consumer it builds are untouched by this
 * module.
 *
 * <p>Same shape as {@code KafkaConsumerConfig}: manual ack ({@code AckMode.MANUAL_IMMEDIATE}),
 * {@code isolation.level=read_committed} (this topic's producer - the ledger - IS transactional,
 * unlike {@code payments.payment-requested.v1}'s outbox/Debezium path, so this setting is not a
 * no-op here the way M4's comment notes it is for the payment path), and
 * {@code ErrorHandlingDeserializer} wrapping {@code KafkaAvroDeserializer} (ADR-0006 category C).
 */
@Configuration
public class RefundKafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, FundsReserved> fundsReservedConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${psp-connector.schema-registry.url}") String schemaRegistryUrl) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FundsReserved>
            fundsReservedKafkaListenerContainerFactory(
                    @Qualifier("fundsReservedConsumerFactory")
                            ConsumerFactory<String, FundsReserved> fundsReservedConsumerFactory,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, FundsReserved> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(fundsReservedConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // M15: see KafkaConsumerConfig's identical comment.
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);
        // No CommonErrorHandler override: this module does not introduce a new retryable exception
        // type, so Spring Kafka's default classification (deserialization/conversion failures are
        // non-retryable, ADR-0006 category C) applies unchanged. M8 scope: the real policy is a
        // non-blocking retry chain ending in a DLQ, same documented gap as psp-connector's other
        // consumer.
        return factory;
    }
}
