package com.example.psp.paymentapi.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
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
 * Producer wiring for {@code merchants.merchant-config-changed.v1} (M10) - the only topic this
 * service produces to <b>directly</b>. {@code payments.payment-requested.v1} still goes through
 * the M6 outbox and Debezium; see
 * {@link com.example.psp.paymentapi.domain.port.MerchantConfigPublisher} for why this one does
 * not.
 *
 * <p>A separate {@link ProducerFactory}/{@link KafkaTemplate} pair rather than a reconfiguration
 * of {@link KafkaProducerConfig}'s: that one still carries {@code application.yml}'s
 * {@code value-serializer: JsonSerializer} for the retired M3 adapter
 * ({@code adapters.out.kafka.KafkaPaymentEventPublisher}, kept compiling for comparison), and
 * flipping it globally would silently change what that class does if it were ever re-enabled.
 * Everything else - {@code acks=all}, {@code enable.idempotence}, retries, batching, compression -
 * is inherited from the same {@link KafkaProperties} block, so this producer's durability
 * behaviour is identical to the rest of the system's by construction, not by copy-paste.
 *
 * <h2>Idempotence and ordering matter more here than on a delete-policy topic</h2>
 *
 * <p>{@code enable.idempotence=true} plus {@code max.in.flight.requests.per.connection<=5}
 * (ADR-0003's global defaults, set in {@code application.yml}) give per-partition ordering across
 * retries. On a compacted topic that is not a nice-to-have: compaction resolves a key to its
 * <b>last</b> record by offset, so a retry that lands out of order does not just deliver events
 * out of sequence, it permanently installs the wrong value - including resurrecting a merchant
 * whose tombstone was overtaken by a re-sent update.
 */
@Configuration
public class MerchantConfigKafkaConfig {

    @Bean
    public ProducerFactory<String, Object> merchantConfigProducerFactory(
            KafkaProperties kafkaProperties,
            @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl) {

        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));

        // Values on this topic are Avro (Confluent wire format); keys stay plain UTF-8 text
        // (ADR-0003), same key convention as every other topic in the system.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // This service is the sole producer/owner of this subject, so letting the first PUT
        // register version 1 is simpler than a manual step - and still governed by the subject's
        // compatibility mode, which infra/compose/register-schemas.sh sets before any producer
        // runs (same argument as SchemaRegistryConfig's for payments.payment-requested.v1).
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);

        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * {@code @Qualifier} is load-bearing: this service now has two
     * {@code ProducerFactory<String, Object>} beans (the M3 JSON one in {@link KafkaProducerConfig}
     * and the Avro one above), and Spring's by-parameter-name fallback does not apply to
     * {@code @Bean} method arguments without {@code -parameters} on the compiler.
     */
    @Bean
    public KafkaTemplate<String, Object> merchantConfigKafkaTemplate(
            @Qualifier("merchantConfigProducerFactory")
                    ProducerFactory<String, Object> merchantConfigProducerFactory) {
        return new KafkaTemplate<>(merchantConfigProducerFactory);
    }
}
