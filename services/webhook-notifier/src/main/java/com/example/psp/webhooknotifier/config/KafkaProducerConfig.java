package com.example.psp.webhooknotifier.config;

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
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Producer wiring - same pattern as psp-connector's {@code KafkaProducerConfig} - plus the
 * {@link TaskScheduler} bean {@code adapters.out.kafka.KafkaWebhookDeliveryPublisher} uses to
 * schedule a non-blocking retry hop. See that class's javadoc for why a scheduler thread pool,
 * not the Kafka consumer thread, is what waits out a retry delay.
 *
 * <h2>M9 Phase 2 - two producer factories, deliberately</h2>
 *
 * <p>{@code webhooks.webhook-delivery-requested.v2} and its three retry tiers are now Avro
 * ({@link #webhookDeliveryAvroKafkaTemplate}); the terminal {@code .v2.dlq} stays on the same
 * {@code spring.kafka.producer.value-serializer} (Spring Kafka's {@code JsonSerializer}) this
 * service used for every topic through M8 ({@link #webhookDeliveryDlqKafkaTemplate}). This is not
 * an oversight - it is the whole point of leaving the DLQ where it was: Spring Kafka's
 * {@code JsonSerializer} special-cases a {@code byte[]} value and writes it through completely
 * unchanged (no Jackson involved at all), which is exactly what lets
 * {@code DeadLetterPublishingRecoverer} republish the raw, still-undeserializable bytes of a
 * genuine poison pill without needing a schema for them - see {@code KafkaConsumerConfig}'s
 * {@code executorKafkaListenerContainerFactory} for where that recoverer is wired to THIS DLQ
 * template, not the Avro one. {@code KafkaAvroSerializer} has no equivalent escape hatch: it needs
 * an Avro schema for whatever object it is handed, so pointing the recoverer at it would either
 * throw on every poison pill or (worse) silently misrepresent the bad bytes as a "bytes"-typed
 * Avro value - neither preserves M8's documented behaviour. See
 * {@code adapters.out.kafka.KafkaWebhookDeliveryPublisher}'s javadoc for how a single publisher
 * picks between the two templates per send, and the README's M9 Phase 2 section for why this
 * chain was cut to a NEW {@code .v2} topic set rather than migrated in place.
 */
@Configuration
public class KafkaProducerConfig {

    /**
     * Backs {@link #webhookDeliveryDlqKafkaTemplate} - unchanged from M8: builds from
     * {@code spring.kafka.producer.*} in {@code application.yml}, whose
     * {@code value-serializer} is still {@code org.springframework.kafka.support.serializer.JsonSerializer}.
     */
    @Bean
    public ProducerFactory<String, Object> webhookDeliveryDlqProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);
        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    /**
     * The byte-tolerant template used for exactly one destination:
     * {@code webhook-notifier.kafka.dlq-topic} - both the application-level exhausted-retry/
     * non-retryable publish ({@code KafkaWebhookDeliveryPublisher#send}) and, indirectly, the
     * container-level {@code DeadLetterPublishingRecoverer} wired to it in
     * {@code KafkaConsumerConfig}.
     */
    @Bean
    public KafkaTemplate<String, Object> webhookDeliveryDlqKafkaTemplate(
            @Qualifier("webhookDeliveryDlqProducerFactory") ProducerFactory<String, Object> producerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);
        // M15: hand-built bean, so Boot's spring.kafka.template.observation-enabled /
        // spring.kafka.listener.observation-enabled property never reaches it - see
        // infra/compose/README.md's M15 section.
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
    }

    /**
     * Backs {@link #webhookDeliveryAvroKafkaTemplate}. Built by hand (not
     * {@code spring.kafka.producer.value-serializer} in YAML, unlike psp-connector's/ledger's
     * single-producer equivalents) because this service needs a SECOND, differently-serialized
     * producer alongside the JSON one above, and Spring Boot's {@link KafkaProperties} only
     * describes one {@code spring.kafka.producer.*} block.
     */
    @Bean
    public ProducerFactory<String, Object> webhookDeliveryAvroProducerFactory(
            KafkaProperties kafkaProperties, @Value("${webhook-notifier.schema-registry.url}") String schemaRegistryUrl) {
        Map<String, Object> producerProps = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        producerProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        // spring.json.add.type.headers doesn't apply to KafkaAvroSerializer - drop it rather than
        // let a Json-only property silently do nothing.
        producerProps.remove("spring.json.add.type.headers");
        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    /**
     * The Avro template used for {@code webhook-notifier.kafka.delivery-requested-topic} and all
     * three {@code retry-*-topic} properties - everything on the chain except the DLQ.
     */
    @Bean
    public KafkaTemplate<String, Object> webhookDeliveryAvroKafkaTemplate(
            @Qualifier("webhookDeliveryAvroProducerFactory") ProducerFactory<String, Object> producerFactory,
            ObservationRegistry observationRegistry) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);
        // M15: hand-built bean, so Boot's spring.kafka.template.observation-enabled /
        // spring.kafka.listener.observation-enabled property never reaches it - see
        // infra/compose/README.md's M15 section.
        template.setObservationRegistry(observationRegistry);
        template.setObservationEnabled(true);
        return template;
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
