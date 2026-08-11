package com.example.psp.webhooknotifier.config;

import java.util.Map;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Producer wiring - same pattern as psp-connector's {@code KafkaProducerConfig} - plus the
 * {@link TaskScheduler} bean {@code adapters.out.kafka.KafkaWebhookDeliveryPublisher} uses to
 * schedule a non-blocking retry hop. See that class's javadoc for why a scheduler thread pool,
 * not the Kafka consumer thread, is what waits out a retry delay.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> webhookDeliveryProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);
        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(
            ProducerFactory<String, Object> webhookDeliveryProducerFactory) {
        return new KafkaTemplate<>(webhookDeliveryProducerFactory);
    }

    /**
     * Small dedicated pool for scheduled retry-hop publishes. Sized modestly (4 threads): each
     * scheduled task does almost no work itself (build a record, hand it to
     * {@code KafkaTemplate#send}, which is itself async) and only needs to exist somewhere that is
     * NOT a Kafka consumer poll thread - see {@code KafkaWebhookDeliveryPublisher}'s javadoc for
     * why that separation is the entire point.
     */
    @Bean
    public TaskScheduler webhookRetryTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(4);
        scheduler.setThreadNamePrefix("webhook-retry-scheduler-");
        scheduler.initialize();
        return scheduler;
    }
}
