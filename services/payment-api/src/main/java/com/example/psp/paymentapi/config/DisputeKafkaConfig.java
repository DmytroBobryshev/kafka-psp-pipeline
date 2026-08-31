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
 * Producer wiring for {@code disputes.dispute-opened.v1} (M13) - the second topic this service
 * produces to directly, alongside {@code merchants.merchant-config-changed.v1}. A separate
 * {@link ProducerFactory}/{@link KafkaTemplate} pair, own {@code @Qualifier}, following {@code
 * MerchantConfigKafkaConfig}'s exact pattern rather than reusing its beans: this service now has
 * THREE {@code ProducerFactory<String, Object>} beans (the retired M3 JSON one, the M10 Avro one,
 * this one), so a fourth reason - not just a third - to need a qualifier, and each topic keeps
 * its own producer identity for exactly the reason {@code MerchantConfigKafkaConfig}'s javadoc
 * gives: reconfiguring a shared bean for one topic risks silently changing another's behaviour.
 *
 * <h2>{@code max.request.size} is deliberately NOT overridden here</h2>
 *
 * <p>Every property this factory does not set falls back to the Kafka client's own default -
 * including {@code max.request.size=1048576} (1 MiB). That default is left alone on purpose: the
 * claim-check pattern's entire justification is that a producer should never need to raise it
 * just to fit an occasional large attachment. See services/payment-api/README.md's "M13: claim
 * check, measured" section for the real {@code RecordTooLargeException} this default produces
 * when the claim-check decision is bypassed.
 */
@Configuration
public class DisputeKafkaConfig {

    @Bean
    public ProducerFactory<String, Object> disputeProducerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // This service is the sole producer/owner of this subject - same reasoning as
        // MerchantConfigKafkaConfig's identical line.
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> disputeKafkaTemplate(
            @Qualifier("disputeProducerFactory") ProducerFactory<String, Object> disputeProducerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(disputeProducerFactory);
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }
}
