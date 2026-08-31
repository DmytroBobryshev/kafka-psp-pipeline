package com.example.psp.analytics.config;

import com.example.psp.common.events.avro.DisputeOpened;
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
 * Consumer wiring for {@code adapters.in.kafka.DisputeOpenedListener} (M13) - a plain,
 * single-record {@code @KafkaListener}, independent of both the Kafka Streams application
 * ({@code config.KafkaStreamsConfig}) and the batch listener ({@code config.
 * BatchListenerKafkaConfig}). Mirrors payment-api's {@code PaymentStatusViewKafkaConfig}: the same
 * Avro {@code ConsumerFactory} pattern ({@code ErrorHandlingDeserializer} wrapping {@link
 * KafkaAvroDeserializer}, {@code specific.avro.reader=true}) and the same "no DLQ, zero-retry
 * {@link DefaultErrorHandler}, log and skip" shape - see {@code adapters.in.kafka.
 * DisputeOpenedListener}'s javadoc for why this consumer carries no DLQ.
 */
@Configuration
public class DisputeKafkaConfig {

    @Bean
    public ConsumerFactory<String, DisputeOpened> disputeOpenedConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${analytics.schema-registry.url}") String schemaRegistryUrl,
            @Value("${analytics.disputes.group-id}") String groupId) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

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
    public ConcurrentKafkaListenerContainerFactory<String, DisputeOpened>
            disputeOpenedKafkaListenerContainerFactory(
                    ConsumerFactory<String, DisputeOpened> disputeOpenedConsumerFactory,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, DisputeOpened> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(disputeOpenedConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);

        // No DLQ (see DisputeOpenedListener's javadoc) - zero retries, log and skip via the
        // default (no-recoverer) DefaultErrorHandler.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }
}
