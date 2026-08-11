package com.example.psp.paymentapi.config;

import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Explicit producer wiring for {@code adapters/out/kafka} (M3).
 *
 * <p>Spring Boot would auto-configure a {@code KafkaTemplate<Object, Object>} from
 * {@code spring.kafka.producer.*} on its own, but its generics don't line up cleanly with the
 * {@code KafkaTemplate<String, Object>} {@link com.example.psp.paymentapi.adapters.out.kafka.KafkaPaymentEventPublisher}
 * wants to inject. Building the {@link ProducerFactory} and {@link KafkaTemplate} beans here -
 * still from the same {@link KafkaProperties}, so every setting under {@code application.yml}'s
 * {@code spring.kafka.producer} block (acks, retries, idempotence, batching, compression) still
 * applies - makes the wiring unambiguous instead of relying on generic-type inference.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, Object> paymentProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);
        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    /**
     * {@code @Qualifier} added in M10: {@code config.MerchantConfigKafkaConfig} introduced a
     * second {@code ProducerFactory<String, Object>} (Avro-serializing, for
     * {@code merchants.merchant-config-changed.v1}), so the type alone no longer identifies a
     * bean. Spring's by-parameter-name fallback does not apply to {@code @Bean} method arguments
     * unless the build passes {@code -parameters}, which this one does not - so the qualifier is
     * the fix, not a stylistic preference.
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(
            @Qualifier("paymentProducerFactory") ProducerFactory<String, Object> paymentProducerFactory) {
        return new KafkaTemplate<>(paymentProducerFactory);
    }
}
