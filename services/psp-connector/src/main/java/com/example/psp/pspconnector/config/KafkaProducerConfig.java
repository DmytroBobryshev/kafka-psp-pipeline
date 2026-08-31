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

    // ============================================================================================
    // M17: DLQ replay republisher (adapters.out.kafka.KafkaDlqRepublisher) - a dedicated, plain
    // byte-array producer, deliberately NOT the paymentStatusProducerFactory/kafkaTemplate above.
    // Replay republishes the DLQ record's raw value bytes unchanged (see KafkaDlqRepublisher's
    // javadoc): KafkaAvroSerializer needs an Avro-typed object to encode and has no escape hatch
    // for a byte[] it was never asked to decode in the first place - the same reasoning
    // webhook-notifier's KafkaProducerConfig documents for its own two-template split.
    // ============================================================================================

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
