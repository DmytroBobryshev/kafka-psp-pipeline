package com.example.psp.paymentapi.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.micrometer.observation.ObservationRegistry;
import java.time.Clock;
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

/**
 * Producer wiring for {@code adapters.out.kafka.KafkaPaymentExpirationPublisher} (M22) - the
 * SECOND topic this service produces to directly (after {@code config.MerchantConfigKafkaConfig}'s
 * {@code merchants.merchant-config-changed.v1}), and mirrors that class's reasoning verbatim: a
 * dedicated {@link ProducerFactory}/{@link KafkaTemplate} pair rather than reusing
 * {@code config.KafkaProducerConfig}'s (still JSON-serializing, for the retired M3 adapter) or
 * {@code MerchantConfigKafkaConfig}'s (a distinct bean identity, even though the serializer
 * config is byte-for-byte the same) - {@code @Qualifier} is what disambiguates a THIRD
 * {@code ProducerFactory<String, Object>} bean from the other two.
 *
 * <p>Also declares this service's {@link Clock} bean: {@code application.ExpirePaymentsUseCase}
 * takes one by constructor injection rather than calling {@code Instant.now()} inline, so its own
 * unit test can drive a fixed instant instead of a real-time race - same "a bean, not an inline
 * call, so the test can substitute it" reasoning as analytics' {@code KafkaStreamsConfig
 * #analyticsClock}.
 */
@Configuration
public class PaymentExpirationKafkaConfig {

    @Bean
    public ProducerFactory<String, Object> paymentExpirationProducerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // This service does not own the payments.payment-status-changed.v1 subject
        // (psp-connector's producer registered version 1 first), so auto-registration here is
        // inert in practice - kept true anyway for the same "no manual registration step" reason
        // MerchantConfigKafkaConfig gives, and because a compatible EXPIRED-carrying schema was
        // already registered by the time this producer ever sends (this schema IS that same one,
        // unchanged - EXPIRED is a value on an existing "status" string field, not a new field).
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> paymentExpirationKafkaTemplate(
            @Qualifier("paymentExpirationProducerFactory")
                    ProducerFactory<String, Object> paymentExpirationProducerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(paymentExpirationProducerFactory);
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public Clock paymentApiClock() {
        return Clock.systemUTC();
    }
}
