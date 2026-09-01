package com.example.psp.pspconnector.config;

import com.example.psp.common.events.avro.ProviderStatusQuery;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

@Configuration
public class ProviderStatusKafkaConfig {

    @Bean
    public ConsumerFactory<String, ProviderStatusQuery> providerStatusQueryConsumerFactory(
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
    public ConcurrentKafkaListenerContainerFactory<String, ProviderStatusQuery>
            providerStatusQueryKafkaListenerContainerFactory(
                    @Qualifier("providerStatusQueryConsumerFactory")
                            ConsumerFactory<String, ProviderStatusQuery> providerStatusQueryConsumerFactory,
                    KafkaTemplate<String, Object> kafkaTemplate,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, ProviderStatusQuery> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(providerStatusQueryConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);
        // THE bean that makes @SendTo work - see class javadoc.
        factory.setReplyTemplate(kafkaTemplate);
        return factory;
    }
}
