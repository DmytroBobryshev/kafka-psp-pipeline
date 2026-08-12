package com.example.psp.pspconnector.config;

import io.micrometer.observation.ObservationRegistry;
import java.util.Map;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Explicit producer wiring for {@code adapters/out/kafka} - same rationale as {@code payment-api}'s
 * {@code KafkaProducerConfig}: building the {@link ProducerFactory}/{@link KafkaTemplate} beans by
 * hand from {@link KafkaProperties} keeps the {@code KafkaTemplate<String, Object>} generics
 * {@link com.example.psp.pspconnector.adapters.out.kafka.KafkaPaymentStatusPublisher} wants
 * unambiguous, while every setting under {@code spring.kafka.producer} in {@code application.yml}
 * (acks, idempotence, batching, compression) still applies.
 */
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
        // M15: hand-built bean, so Boot's spring.kafka.template.observation-enabled property never
        // reaches it - see infra/compose/README.md's M15 section. This is what makes
        // KafkaPaymentStatusPublisher's send() inject a real W3C traceparent header, continuing
        // the trace this consumer's inbound record's header started (see KafkaConsumerConfig).
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }
}
