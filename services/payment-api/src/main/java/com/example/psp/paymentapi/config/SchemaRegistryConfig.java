package com.example.psp.paymentapi.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
