package com.example.psp.paymentapi.config;

import io.micrometer.observation.ObservationRegistry;
import java.util.Map;
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
    public ProducerFactory<String, Object> paymentProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);
        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(
            @Qualifier("paymentProducerFactory") ProducerFactory<String, Object> paymentProducerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(paymentProducerFactory);
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }
}
