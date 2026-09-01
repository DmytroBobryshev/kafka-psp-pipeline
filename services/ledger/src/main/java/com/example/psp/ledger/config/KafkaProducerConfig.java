package com.example.psp.ledger.config;

import io.micrometer.observation.ObservationRegistry;
import jakarta.persistence.EntityManagerFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> ledgerEntryProducerFactory(
            KafkaProperties kafkaProperties, LedgerProperties ledgerProperties) {
        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);

        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);

        DefaultKafkaProducerFactory<String, Object> factory =
                new DefaultKafkaProducerFactory<>(producerProps);

        factory.setTransactionIdPrefix(ledgerProperties.kafka().transactionalIdPrefix());

        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(
            ProducerFactory<String, Object> ledgerEntryProducerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(ledgerEntryProducerFactory);
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(
            ProducerFactory<String, Object> ledgerEntryProducerFactory) {
        return new KafkaTransactionManager<>(ledgerEntryProducerFactory);
    }

    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
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
