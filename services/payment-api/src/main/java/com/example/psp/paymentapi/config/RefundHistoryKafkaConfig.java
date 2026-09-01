package com.example.psp.paymentapi.config;

import com.example.psp.common.events.avro.FundsReserved;
import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.common.events.avro.RefundStatusChanged;
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
 * M23's consumer wiring for the refund trail's four read-model listeners
 * ({@code adapters.in.kafka.RefundStatusChangedListener}/{@code RefundCompletedListener}/
 * {@code RefundFailedListener}/{@code RefundFundsReservedListener}) - four independent, fixed
 * work-sharing consumer groups (all named {@code payment-api.refund-*}), one per topic, same shape
 * as {@code PaymentStatusViewKafkaConfig}/{@code MerchantViewKafkaConfig}: {@code
 * ErrorHandlingDeserializer} wrapping {@link KafkaAvroDeserializer}, manual-immediate acks, no DLQ
 * (a derived, lossy read-model projection, ADR-0006 - same documented scope boundary as those two
 * classes).
 *
 * <p>{@code auto.offset.reset=earliest}, explicitly set on all four - unlike
 * {@code payment-status-view}'s inherited default, these ARE new consumer groups added onto topics
 * that already carry data from before this projection existed (refund-completed/failed/
 * funds-reserved), so a fresh group must replay from the start or silently miss every refund saga
 * that ran before this feature shipped. Same reasoning as {@code MerchantViewKafkaConfig}'s
 * identical override for the compacted merchant-config topic.
 */
@Configuration
public class RefundHistoryKafkaConfig {

    @Bean
    public ConsumerFactory<String, RefundStatusChanged> refundStatusViewConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl,
            @Value("${payment-api.kafka.refund-status-view-group-id}") String groupId) {
        return avroConsumerFactory(kafkaProperties, schemaRegistryUrl, groupId);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RefundStatusChanged>
            refundStatusViewKafkaListenerContainerFactory(
                    ConsumerFactory<String, RefundStatusChanged> refundStatusViewConsumerFactory,
                    ObservationRegistry observationRegistry) {
        return containerFactory(refundStatusViewConsumerFactory, observationRegistry);
    }

    @Bean
    public ConsumerFactory<String, RefundCompleted> refundCompletedViewConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl,
            @Value("${payment-api.kafka.refund-completed-view-group-id}") String groupId) {
        return avroConsumerFactory(kafkaProperties, schemaRegistryUrl, groupId);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RefundCompleted>
            refundCompletedViewKafkaListenerContainerFactory(
                    ConsumerFactory<String, RefundCompleted> refundCompletedViewConsumerFactory,
                    ObservationRegistry observationRegistry) {
        return containerFactory(refundCompletedViewConsumerFactory, observationRegistry);
    }

    @Bean
    public ConsumerFactory<String, RefundFailed> refundFailedViewConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl,
            @Value("${payment-api.kafka.refund-failed-view-group-id}") String groupId) {
        return avroConsumerFactory(kafkaProperties, schemaRegistryUrl, groupId);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RefundFailed>
            refundFailedViewKafkaListenerContainerFactory(
                    ConsumerFactory<String, RefundFailed> refundFailedViewConsumerFactory,
                    ObservationRegistry observationRegistry) {
        return containerFactory(refundFailedViewConsumerFactory, observationRegistry);
    }

    @Bean
    public ConsumerFactory<String, FundsReserved> refundFundsReservedViewConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl,
            @Value("${payment-api.kafka.refund-funds-reserved-view-group-id}") String groupId) {
        return avroConsumerFactory(kafkaProperties, schemaRegistryUrl, groupId);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, FundsReserved>
            refundFundsReservedViewKafkaListenerContainerFactory(
                    ConsumerFactory<String, FundsReserved> refundFundsReservedViewConsumerFactory,
                    ObservationRegistry observationRegistry) {
        return containerFactory(refundFundsReservedViewConsumerFactory, observationRegistry);
    }

    /** Shared Avro {@code ConsumerFactory} shape - identical props across all four topics, only group.id differs. */
    private static <T> ConsumerFactory<String, T> avroConsumerFactory(
            KafkaProperties kafkaProperties, String schemaRegistryUrl, String groupId) {
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

    /** Shared container shape - manual-immediate acks, zero-retry no-DLQ error handler. */
    private static <T> ConcurrentKafkaListenerContainerFactory<String, T> containerFactory(
            ConsumerFactory<String, T> consumerFactory, ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(0L, 0L)));
        return factory;
    }
}
