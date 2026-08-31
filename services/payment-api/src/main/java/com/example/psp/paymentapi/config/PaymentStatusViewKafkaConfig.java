package com.example.psp.paymentapi.config;

import com.example.psp.common.events.avro.PaymentStatusChanged;
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
 * M19's consumer wiring for {@code adapters.in.kafka.PaymentStatusChangedListener} - a NEW,
 * ordinary work-sharing consumer group ({@code payment-api.status-view.v1}), deliberately NOT
 * built the way {@code ReplyingKafkaConfig}'s reply consumer is: that one mints a unique group id
 * per instance so every instance sees every reply (a request/reply correlation problem, see that
 * class's javadoc). This listener has no such requirement - it is a plain, idempotent read-model
 * projection, so a fixed, shared group id is not just fine but correct: with more than one
 * payment-api instance, the topic's partitions split normally across them, exactly as intended for
 * a real work-sharing consumer group.
 *
 * <p>Mirrors {@code ReplyingKafkaConfig}'s Avro {@code ConsumerFactory} pattern
 * ({@code ErrorHandlingDeserializer} wrapping {@link KafkaAvroDeserializer},
 * {@code specific.avro.reader=true}) plus a hand-built {@link ConcurrentKafkaListenerContainerFactory}
 * in the shape {@code webhook-notifier}'s {@code config.KafkaConsumerConfig} already established
 * for its own planner listener on this identical topic: manual-immediate acks, and a
 * zero-retry {@link DefaultErrorHandler} with no recoverer - see
 * {@code adapters.in.kafka.PaymentStatusChangedListener}'s javadoc for why this listener carries
 * no DLQ.
 */
@Configuration
public class PaymentStatusViewKafkaConfig {

    @Bean
    public ConsumerFactory<String, PaymentStatusChanged> paymentStatusViewConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl,
            @Value("${payment-api.kafka.payment-status-view-group-id}") String groupId) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // ADR-0006 category C - "MUST be configured on every consumer factory".
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // specific.avro.reader=true: hand the listener the generated class, not a schema-less
        // GenericRecord - same requirement as every other Avro consumer in this system.
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChanged>
            paymentStatusViewKafkaListenerContainerFactory(
                    ConsumerFactory<String, PaymentStatusChanged> paymentStatusViewConsumerFactory,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentStatusChanged> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(paymentStatusViewConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // M15: hand-built bean, so Boot's spring.kafka.listener.observation-enabled property
        // never reaches it - same reasoning as every other explicit container factory in this
        // system.
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);

        // No DLQ for this topic (see PaymentStatusChangedListener's javadoc) - zero retries, log
        // and skip via the default (no-recoverer) DefaultErrorHandler.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }
}
