package com.example.psp.pspconnector.config;

import com.example.psp.common.events.avro.ProviderStatusQuery;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.observation.ObservationRegistry;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

/**
 * M12's responder-side consumer wiring for {@code psp.provider-status-query.v1} - the "server"
 * half of the request-reply pair described in {@code adapters.in.kafka.ProviderStatusQueryListener}'s
 * javadoc and services/psp-connector/README.md's M12 section.
 *
 * <p>Same shape as {@code KafkaConsumerConfig}/{@code RefundKafkaConsumerConfig}: manual ack,
 * {@code ErrorHandlingDeserializer} wrapping {@code KafkaAvroDeserializer} (ADR-0006 category C),
 * {@code specific.avro.reader=true}. This topic's consumer group ({@code psp-connector.v1} -
 * unchanged, it shares the group id every other listener in this class uses, per
 * docs/diagrams/topic-map.md's consumer-groups table: "psp-connector.v1 | psp-connector |
 * payments.payment-requested.v1, refunds.funds-reserved.v1, psp.provider-status-query.v1") is a
 * completely different situation from realtime-gateway's unique-per-instance group (M12's OTHER
 * half): a status query genuinely should be load-split across psp-connector instances - any
 * instance can answer it from the shared psp_connector database, so ordinary consumer-group
 * load-balancing is exactly the right tool here, unlike the fan-out case.
 *
 * <p>{@link #providerStatusQueryKafkaListenerContainerFactory}'s {@code setReplyTemplate} is what
 * turns a plain {@code @KafkaListener} method into a request-reply responder: Spring Kafka reads
 * the inbound record's {@code KafkaHeaders.REPLY_TOPIC} header (set by the requester's {@code
 * ReplyingKafkaTemplate} - see payment-api's {@code config.ReplyingKafkaConfig}), sends the
 * listener method's return value to that topic via this template, and copies {@code
 * KafkaHeaders.CORRELATION_ID} (and {@code REPLY_PARTITION}, if present) from request to reply
 * automatically - none of that correlation plumbing is hand-written here. The template reused is
 * the SAME {@code KafkaTemplate<String, Object>} bean {@code KafkaProducerConfig} already builds
 * for {@code payments.payment-status-changed.v1} - its producer factory already Avro-encodes via
 * {@code KafkaAvroSerializer} with {@code auto.register.schemas=true} (application.yml), which is
 * exactly what a NEW subject ({@code psp.provider-status-reply.v1-value}) needs on its first ever
 * publish.
 */
@Configuration
public class ProviderStatusKafkaConfig {

    @Bean
    public ConsumerFactory<String, ProviderStatusQuery> providerStatusQueryConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${psp-connector.schema-registry.url}") String schemaRegistryUrl) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ProviderStatusQuery>
            providerStatusQueryKafkaListenerContainerFactory(
                    @Qualifier("providerStatusQueryConsumerFactory")
                            ConsumerFactory<String, ProviderStatusQuery> providerStatusQueryConsumerFactory,
                    KafkaTemplate<String, Object> kafkaTemplate,
                    ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, ProviderStatusQuery> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(providerStatusQueryConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // M15: see KafkaConsumerConfig's identical comment. The reply itself (sent via
        // kafkaTemplate, injected above) is traced too - that template's own observation is
        // enabled in KafkaProducerConfig.
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);
        // THE bean that makes @SendTo work - see class javadoc.
        factory.setReplyTemplate(kafkaTemplate);
        return factory;
    }
}
