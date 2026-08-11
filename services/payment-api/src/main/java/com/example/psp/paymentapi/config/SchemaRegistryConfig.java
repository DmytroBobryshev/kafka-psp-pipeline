package com.example.psp.paymentapi.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * M9 Phase 1: payment-api never talks to Kafka directly (the M6 outbox is the whole point - see
 * {@code adapters.out.outbox.OutboxPaymentEventPublisher}), but it DOES now talk to Schema
 * Registry directly, over plain HTTP, to Avro-encode the {@code payments.payment-requested.v1}
 * outbox payload into the exact Confluent wire format (1 magic byte + 4-byte schema id + Avro
 * binary) BEFORE it is ever written to {@code outbox_event.payload}.
 *
 * <p>{@link KafkaAvroSerializer} is used purely as an encoder here: its
 * {@code serialize(topic, record)} method registers-or-looks-up the schema against the registry
 * (subject {@code payments.payment-requested.v1-value}, {@code TopicNameStrategy} - ADR-0001) and
 * returns the wire bytes. No {@code ProducerRecord}, no Kafka broker connection, and no
 * {@code KafkaTemplate} are ever involved in producing this class's output - see the README's M9
 * section for the full outbox-serialization decision and the alternatives rejected.
 *
 * <p>{@code auto.register.schemas=true}: this service is this schema's only owner/producer, so
 * letting the very first {@code POST /api/payments} call register version 1 is simpler than a
 * separate manual registration step, and it is still governed by the subject's compatibility
 * mode - {@code infra/compose/register-schemas.sh} sets that BEFORE any producer runs, so even
 * this first registration is under policy, not exempt from it.
 */
@Configuration
public class SchemaRegistryConfig {

    @Bean
    public KafkaAvroSerializer paymentRequestedAvroSerializer(
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl) {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(
                Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                        KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true),
                false); // isKey=false - this serializer only ever encodes the record VALUE.
        return serializer;
    }
}
