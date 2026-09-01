package com.example.psp.paymentapi.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.micrometer.observation.ObservationRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class MerchantConfigKafkaConfig {

    @Bean
    public ProducerFactory<String, Object> merchantConfigProducerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> merchantConfigKafkaTemplate(
            @Qualifier("merchantConfigProducerFactory")
                    ProducerFactory<String, Object> merchantConfigProducerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(merchantConfigProducerFactory);
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }
}
