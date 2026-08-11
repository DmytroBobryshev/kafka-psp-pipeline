package com.example.psp.paymentapi.config;

import com.example.psp.common.events.avro.ProviderStatusQuery;
import com.example.psp.common.events.avro.ProviderStatusReply;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
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

/**
 * M12's requester-side wiring: {@code ReplyingKafkaTemplate} for the synchronous provider-status
 * check (ADR-0004's "synchronous need -&gt; Kafka request-reply" carve-out - see the README's M12
 * section for the full ADR-0004 argument). This is payment-api's FIRST real Kafka producer AND
 * consumer since M6 replaced the M3 direct producer with the transactional outbox - every other
 * write this service makes to Kafka goes through {@code adapters.out.outbox}, which does no
 * network I/O to a broker at all. Request-reply cannot go through the outbox: the whole point is
 * a synchronous answer inside one REST call, and the outbox's contract is "eventually, out of
 * process" (see {@code adapters.out.outbox.OutboxPaymentEventPublisher}'s javadoc). That is
 * exactly ADR-0004's documented exception, not a quiet reintroduction of service-to-service REST.
 *
 * <h2>The reply topic's partitions and this service's OWN broadcast-adjacent problem</h2>
 *
 * <p>{@code psp.provider-status-reply.v1} has 6 partitions (docs/diagrams/topic-map.md), keyed by
 * {@code paymentId} - "correlation only, no ordering semantics". A {@link ReplyingKafkaTemplate}
 * only ever sees a reply if ITS OWN reply-listener container is assigned the partition that reply
 * lands on. With more than one payment-api instance, sharing one ordinary consumer group across
 * those instances would split the 6 partitions between them (the exact same
 * load-splitting-not-fan-out mechanic {@code realtime-gateway} is built around, M12's other half -
 * see that module's README) - so a reply could land on a partition owned by an instance that
 * never sent the matching request, and the instance that DID send it, and is holding the
 * {@code CompletableFuture} a real HTTP thread is blocked on, would simply time out despite a
 * perfectly good reply having been produced.
 *
 * <p>The fix mirrors {@code realtime-gateway}'s: {@link #providerStatusReplyGroupId} is unique
 * PER INSTANCE (hostname + a UUID minted once at class-load), so every payment-api instance's
 * reply container is assigned ALL 6 partitions of the reply topic - guaranteeing every reply this
 * service's cluster produces is visible to every instance, regardless of which one sent the
 * request. {@link ReplyingKafkaTemplate#setSharedReplyTopic} is set to {@code true} because that
 * is now structurally always true here: every instance's {@code ReplyingKafkaTemplate} looks up
 * the incoming record's correlation id in ITS OWN pending-request map and finds a match only if
 * IT sent that request; every other instance finds no match and DEBUG-logs and discards the
 * record instead of WARN-logging it as "reply with no pending request" (the default, correctly
 * noisy behaviour when a reply topic is NOT shared). The traded-off cost: every instance consumes
 * every reply produced anywhere in the fleet, discarding most of them - acceptable for a topic
 * capped at 1 hour of retention and answering a low-volume, human-triggered status check, not
 * worth building the more surgical alternative (routing each request's reply to a specific
 * partition via {@code KafkaHeaders.REPLY_PARTITION}, which needs a stable instance-to-partition
 * assignment this stateless demo has no natural place to keep).
 *
 * <h2>What happens when a reply never arrives</h2>
 *
 * <p>{@link ReplyingKafkaTemplate#setDefaultReplyTimeout} bounds how long
 * {@code adapters.out.kafka.ProviderStatusRequestGateway} blocks the calling HTTP thread. A
 * timeout most commonly means: psp-connector is down or lagging, the query never reached it, or
 * the reply was produced but landed on a partition assignment race during a rebalance. The
 * returned future completes exceptionally with a
 * {@link org.springframework.kafka.requestreply.KafkaReplyTimeoutException}, which the gateway
 * translates into {@code domain.exception.ProviderStatusTimeoutException} - a clean domain-level
 * signal, not a raw Kafka type, surfacing at the REST layer as 504 Gateway Timeout. See the
 * gateway class and the README for the timeout VALUE chosen and why.
 */
@Configuration
public class ReplyingKafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(ReplyingKafkaConfig.class);

    // Minted once per JVM, exactly like realtime-gateway's consumer group.id (see that module's
    // config.KafkaConsumerConfig) - a fresh value every restart is fine and expected: this
    // consumer's ONLY job is to see replies to requests THIS process is about to send, which by
    // definition never span a restart (an in-flight ReplyingKafkaTemplate future cannot survive
    // its own JVM dying anyway).
    private static final String INSTANCE_SUFFIX = UUID.randomUUID().toString();

    @Bean
    public ProducerFactory<String, ProviderStatusQuery> providerStatusQueryProducerFactory(
            KafkaProperties kafkaProperties, @Value("${payment-api.schema-registry.url}") String schemaRegistryUrl) {
        // Starts from spring.kafka.producer.* (application.yml) for the generic settings shared
        // with the M3 producer (acks, retries, idempotence, batching) - but that YAML block's
        // value-serializer is JsonSerializer (config.KafkaProducerConfig's M3 producer, still
        // built unconditionally today - see that class's javadoc), and YAML has only one
        // spring.kafka.producer.value-serializer slot. Rather than fight over that one slot,
        // this bean OVERRIDES it explicitly in Java, the same "override specific keys on top of a
        // shared YAML base" pattern psp-connector's config.KafkaConsumerConfig already uses for
        // its deserializer classes - see application.yml's comment on this producer block for the
        // full reasoning.
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // payment-api is this schema's only producer, so letting the first query register version
        // 1 is simpler than a manual step - same reasoning as SchemaRegistryConfig's M9 comment,
        // still governed by register-schemas.sh's subject-level compatibility mode.
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

        // latest, not earliest (application.yml sets this too - restated here since it is the
        // load-bearing setting this bean depends on): a brand-new group.id has no committed
        // offset regardless, and every reply older than "this process just started" belongs to a
        // request this process could not possibly have sent - see class javadoc.
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
            @Value("${payment-api.kafka.provider-status-reply-topic}") String replyTopic) {
        ContainerProperties containerProperties = new ContainerProperties(replyTopic);
        // ReplyingKafkaTemplate installs its OWN internal MessageListener on this container (it
        // matches each record's KafkaHeaders.CORRELATION_ID against its pending-request map) -
        // this bean exists purely to be handed to the template below, never to carry a
        // @KafkaListener of our own.
        return new KafkaMessageListenerContainer<>(providerStatusReplyConsumerFactory, containerProperties);
    }

    @Bean
    public ReplyingKafkaTemplate<String, ProviderStatusQuery, ProviderStatusReply> providerStatusReplyingKafkaTemplate(
            ProducerFactory<String, ProviderStatusQuery> providerStatusQueryProducerFactory,
            KafkaMessageListenerContainer<String, ProviderStatusReply> providerStatusReplyContainer) {
        ReplyingKafkaTemplate<String, ProviderStatusQuery, ProviderStatusReply> template =
                new ReplyingKafkaTemplate<>(providerStatusQueryProducerFactory, providerStatusReplyContainer);

        // See class javadoc, "The reply topic's partitions and this service's OWN broadcast-
        // adjacent problem": every instance's group.id is unique, so every instance sees every
        // reply and must expect most of them to belong to somebody else.
        template.setSharedReplyTopic(true);

        // See class javadoc, "What happens when a reply never arrives" - and
        // adapters.out.kafka.ProviderStatusRequestGateway / the README for the value's
        // justification.
        template.setDefaultReplyTimeout(Duration.ofSeconds(5));

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
