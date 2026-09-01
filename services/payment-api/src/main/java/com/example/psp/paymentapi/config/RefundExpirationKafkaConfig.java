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

/**
 * Producer wiring for {@code adapters.out.kafka.KafkaRefundExpirationPublisher} (M24) - the refund
 * -path mirror of {@code config.PaymentExpirationKafkaConfig}, same reasoning verbatim: a
 * dedicated {@link ProducerFactory}/{@link KafkaTemplate} pair rather than reusing any of this
 * service's other {@code ProducerFactory<String, Object>} beans - {@code @Qualifier} is what
 * disambiguates yet another one of those from the others.
 *
 * <p>Deliberately does NOT redeclare a {@link java.time.Clock} bean -
 * {@code PaymentExpirationKafkaConfig#paymentApiClock} already provides the single {@code Clock}
 * bean this service context needs; {@code application.ExpireRefundsUseCase} is constructor-wired
 * to that same bean, exactly like {@code application.ExpirePaymentsUseCase} is.
 */
@Configuration
public class RefundExpirationKafkaConfig {

    @Bean
    public ProducerFactory<String, Object> refundExpirationProducerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // This service does not own the refunds.refund-status-changed.v1 subject (psp-connector's
        // producer registered version 1 first), so auto-registration here is inert in practice -
        // kept true anyway for the same reason PaymentExpirationKafkaConfig gives: a compatible
        // EXPIRED-carrying schema was already registered by the time this producer ever sends
        // (this schema IS that same one, unchanged - EXPIRED is a value on an existing "status"
        // string field, not a new field).
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> refundExpirationKafkaTemplate(
            @Qualifier("refundExpirationProducerFactory")
                    ProducerFactory<String, Object> refundExpirationProducerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(refundExpirationProducerFactory);
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }
}
