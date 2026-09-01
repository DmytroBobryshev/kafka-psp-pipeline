package com.example.psp.pspconnector.config;

import io.micrometer.observation.ObservationRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> paymentStatusProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);
        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(
            ProducerFactory<String, Object> paymentStatusProducerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(paymentStatusProducerFactory);
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public ProducerFactory<String, byte[]> dlqReplayProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProps = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    @Bean
    public KafkaTemplate<String, byte[]> dlqReplayKafkaTemplate(
            @Qualifier("dlqReplayProducerFactory") ProducerFactory<String, byte[]> dlqReplayProducerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, byte[]> template = new KafkaTemplate<>(dlqReplayProducerFactory);
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }
}
