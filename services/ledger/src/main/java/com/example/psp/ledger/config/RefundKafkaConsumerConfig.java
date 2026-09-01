package com.example.psp.ledger.config;

import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.common.events.avro.RefundRequested;
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
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class RefundKafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, RefundRequested> refundRequestedConsumerFactory(
            KafkaProperties kafkaProperties, @Value("${ledger.schema-registry.url}") String schemaRegistryUrl) {
        return avroConsumerFactory(kafkaProperties, schemaRegistryUrl);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RefundRequested>
            refundRequestedKafkaListenerContainerFactory(
                    @Qualifier("refundRequestedConsumerFactory")
                            ConsumerFactory<String, RefundRequested> consumerFactory,
                    KafkaTransactionManager<String, Object> kafkaTransactionManager,
                    ObservationRegistry observationRegistry) {
        return containerFactory(consumerFactory, kafkaTransactionManager, observationRegistry);
    }

    @Bean
    public ConsumerFactory<String, RefundCompleted> refundCompletedConsumerFactory(
            KafkaProperties kafkaProperties, @Value("${ledger.schema-registry.url}") String schemaRegistryUrl) {
        return avroConsumerFactory(kafkaProperties, schemaRegistryUrl);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RefundCompleted>
            refundCompletedKafkaListenerContainerFactory(
                    @Qualifier("refundCompletedConsumerFactory")
                            ConsumerFactory<String, RefundCompleted> consumerFactory,
                    KafkaTransactionManager<String, Object> kafkaTransactionManager,
                    ObservationRegistry observationRegistry) {
        return containerFactory(consumerFactory, kafkaTransactionManager, observationRegistry);
    }

    @Bean
    public ConsumerFactory<String, RefundFailed> refundFailedConsumerFactory(
            KafkaProperties kafkaProperties, @Value("${ledger.schema-registry.url}") String schemaRegistryUrl) {
        return avroConsumerFactory(kafkaProperties, schemaRegistryUrl);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RefundFailed>
            refundFailedKafkaListenerContainerFactory(
                    @Qualifier("refundFailedConsumerFactory") ConsumerFactory<String, RefundFailed> consumerFactory,
                    KafkaTransactionManager<String, Object> kafkaTransactionManager,
                    ObservationRegistry observationRegistry) {
        return containerFactory(consumerFactory, kafkaTransactionManager, observationRegistry);
    }

    private <T> ConsumerFactory<String, T> avroConsumerFactory(
            KafkaProperties kafkaProperties, String schemaRegistryUrl) {
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

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> containerFactory(
            ConsumerFactory<String, T> consumerFactory,
            KafkaTransactionManager<String, Object> kafkaTransactionManager,
            ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setKafkaAwareTransactionManager(kafkaTransactionManager);
        factory.setAfterRollbackProcessor(
                new DefaultAfterRollbackProcessor<>(new FixedBackOff(1_000L, 2L)));
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
