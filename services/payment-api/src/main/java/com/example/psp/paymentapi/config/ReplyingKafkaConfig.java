package com.example.psp.paymentapi.config;

import com.example.psp.common.events.avro.ProviderStatusQuery;
import com.example.psp.common.events.avro.ProviderStatusReply;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.micrometer.observation.ObservationRegistry;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

@Configuration
public class ReplyingKafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(ReplyingKafkaConfig.class);

    private static final String INSTANCE_SUFFIX = UUID.randomUUID().toString();

    @Bean
    public ProducerFactory<String, ProviderStatusQuery> providerStatusQueryProducerFactory(
            KafkaProperties kafkaProperties, @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public ConsumerFactory<String, ProviderStatusReply> providerStatusReplyConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);

        String groupId = providerStatusReplyGroupId();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        log.info(
                "payment-api provider-status-reply consumer group.id={} (unique per instance, see"
                        + " class javadoc)",
                groupId);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public KafkaMessageListenerContainer<String, ProviderStatusReply> providerStatusReplyContainer(
            ConsumerFactory<String, ProviderStatusReply> providerStatusReplyConsumerFactory,
            @Value("${payment-api.kafka.provider-status-reply-topic}") String replyTopic,
            ObservationRegistry observationRegistry) {
        ContainerProperties containerProperties = new ContainerProperties(replyTopic);
        containerProperties.setObservationRegistry(observationRegistry);
        containerProperties.setObservationEnabled(true);
        return new KafkaMessageListenerContainer<>(providerStatusReplyConsumerFactory, containerProperties);
    }

    @Bean
    public ReplyingKafkaTemplate<String, ProviderStatusQuery, ProviderStatusReply> providerStatusReplyingKafkaTemplate(
            ProducerFactory<String, ProviderStatusQuery> providerStatusQueryProducerFactory,
            KafkaMessageListenerContainer<String, ProviderStatusReply> providerStatusReplyContainer,
            ObservationRegistry observationRegistry) {
        ReplyingKafkaTemplate<String, ProviderStatusQuery, ProviderStatusReply> template =
                new ReplyingKafkaTemplate<>(providerStatusQueryProducerFactory, providerStatusReplyContainer);

        template.setSharedReplyTopic(true);

        template.setDefaultReplyTimeout(Duration.ofSeconds(5));

        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);

        return template;
    }

    private static String providerStatusReplyGroupId() {
        return "payment-api.replies." + resolveHostname() + "." + INSTANCE_SUFFIX;
    }

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown-host";
        }
    }
}
