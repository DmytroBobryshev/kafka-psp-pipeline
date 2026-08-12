package com.example.psp.realtimegateway.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.observation.ObservationRegistry;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

/**
 * THE central point of this module (module brief). Read this class before anything else in
 * {@code realtime-gateway}.
 *
 * <h2>The broadcast problem</h2>
 *
 * <p>Every gateway instance needs EVERY event on the 7 topics it watches, because each instance
 * holds a completely different set of browser connections - there is no way to know in advance
 * which instance a given browser will connect to, so the only correct answer is "all of them, all
 * the time".
 *
 * <p>A Kafka <b>consumer group is a load-splitting mechanism, not a fan-out mechanism</b>: the
 * group coordinator divides a topic's partitions ACROSS the group's live members so that each
 * partition is owned by exactly one member at a time - the entire point being that N members
 * collectively do 1/N of the work each, not that all N see everything. Two, or twenty, gateway
 * instances sharing one {@code group.id} would each receive only their assigned SLICE of the
 * traffic - a payment's status-changed event would land on whichever instance's consumer happens
 * to own that partition, which is very likely NOT the instance holding the browser connection
 * that is waiting to render it. The browser would simply never see the update, with no error
 * anywhere: the record was consumed, acknowledged, and correctly processed - just by the wrong
 * process.
 *
 * <p>This is the exact same rule M4 measured directly: services/psp-connector/README.md's
 * "Partition / consumer ratio" experiment put 3 consumers on a 3-partition topic (one partition
 * each), started a 4th, and watched it sit at ZERO partitions - <i>"A partition is assigned to
 * exactly one consumer in a group... the fourth consumer is pure standby... it adds zero
 * throughput."</i> That 4th consumer being idle was a harmless, even desirable, property for
 * psp-connector (spare capacity, ready to take over on failure). Apply the SAME mechanic here and
 * it is not harmless at all: a "4th realtime-gateway instance" sharing the group would silently
 * serve zero of the events its own connected browsers are waiting for, while looking completely
 * healthy (consuming, acknowledging, no errors, no lag). The one lesson - "a partition belongs to
 * exactly one group member" - produces a merely suboptimal outcome in one module and a silently
 * broken product feature in this one, depending entirely on whether the workload wants
 * load-splitting or fan-out. That is the whole point of this module.
 *
 * <h2>The fix: a unique {@code group.id} per instance</h2>
 *
 * <p>{@link #INSTANCE_SUFFIX} is a random UUID, generated ONCE per JVM at class-load time.
 * {@link #groupId()} combines it with the resolved hostname (or container/pod name, in Docker/
 * Kubernetes) into {@code realtime-gateway.<hostname>.<uuid>}, matching
 * docs/diagrams/topic-map.md's consumer-groups table
 * ({@code realtime-gateway.<instanceId> | realtime-gateway | payments.*, refunds.* - unique per
 * instance}).
 *
 * <p><b>Why both a stable-per-instance part AND a random part, not just one:</b> hostname alone
 * is not enough - on a single developer laptop (this module's own verification, and every
 * "prove it" run in this codebase happens against {@code localhost}), two instances started as
 * plain {@code java -jar} processes on different ports share the SAME hostname, so hostname-only
 * uniqueness would collide the moment two instances run side by side outside a container. A
 * random UUID alone would work but throws away a genuinely useful piece of information for free:
 * the hostname (or Kubernetes pod name) is what an operator actually reads in
 * {@code kafka-consumer-groups --list} output to tell instances apart at a glance - dropping it
 * would make every group id equally opaque. Combining both keeps the id human-debuggable AND
 * collision-proof regardless of deployment topology.
 *
 * <h2>Consequence: throwaway groups in {@code __consumer_offsets}</h2>
 *
 * <p>Every restart mints a brand-new {@code group.id}, and Kafka never deletes a consumer group's
 * committed offsets on its own - they simply sit in the internal {@code __consumer_offsets} topic
 * until either the group is explicitly deleted or the broker's
 * {@code offsets.retention.minutes} (default 10080 = 7 days) ages them out after the group has no
 * live members. A long-running fleet of gateway instances restarting routinely (rolling deploys,
 * pod evictions, autoscaling) accumulates one abandoned group per restart, forever, unless
 * something reaps them.
 *
 * <p><b>What I would do in production, and which I chose here:</b> two real options exist -
 * (1) lower {@code offsets.retention.minutes} specifically for this pattern (broker-level, or via
 * a dedicated topic/group naming convention an operator can filter on) so abandoned groups expire
 * in hours rather than a week, or (2) sidestep consumer groups entirely for this service by using
 * manual partition assignment ({@code Consumer#assign()} instead of {@code #subscribe()}, e.g.
 * via Spring's {@code @KafkaListener(topicPartitions = ...)}) - no group membership at all means
 * NOTHING is ever written to {@code __consumer_offsets} for this service, which is arguably the
 * cleaner fix since it removes the accumulation at the source rather than merely shortening its
 * lifetime. This module builds the {@code subscribe()}-with-unique-group.id version - it is the
 * approach docs/PLAN.md's M12 brief lists first ("each instance uses a unique group.id (or
 * partition assignment without a group)"), it needs no per-topic partition-count bookkeeping in
 * this config class, and it is the more direct illustration of "group.id is the load-splitting
 * knob" that this module exists to teach. Manual assignment is the documented production
 * alternative, not built here - see the README's M12 section for the same tradeoff spelled out
 * for an operator audience.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    // Minted once per JVM - see class javadoc. A static final, not re-computed per bean method
    // call, so every consumer this class builds (there is only one today) shares the same id for
    // the life of this process.
    private static final String INSTANCE_SUFFIX = UUID.randomUUID().toString();

    @Bean
    public ConsumerFactory<String, Object> realtimeConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${realtime-gateway.schema-registry.url}") String schemaRegistryUrl) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);

        String groupId = groupId();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        log.info(
                "realtime-gateway consumer group.id={} (unique per instance - see this class's"
                        + " javadoc, 'the broadcast problem')",
                groupId);

        // latest, not earliest - restated here from application.yml since it is the load-bearing
        // setting this bean depends on: a brand-new group.id (every restart mints one) has no
        // committed offset regardless, and this service's only job is pushing LIVE events to
        // currently-connected browsers. Nobody is connected yet at startup to see backlog, so
        // "earliest" would replay the ENTIRE 7-day retention of the payment path (12 partitions x
        // 2 topics) for zero observable benefit - pure startup latency and broker I/O. This is a
        // deliberate, documented exception to docs/diagrams/topic-map.md's "every group sets ...
        // auto.offset.reset=earliest" rule, for the same reason payment-api's M12 reply consumer
        // (config.ReplyingKafkaConfig, the OTHER unique-group.id consumer in this system) also
        // overrides it.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        // specific.avro.reader=true - every one of the 7 topics this service subscribes to
        // resolves to its own generated class (PaymentRequested, PaymentStatusChanged,
        // RefundRequested, FundsReserved, RefundCompleted, RefundFailed, ReservationReleased),
        // which is exactly what makes RealtimeEventMapper's pattern-matching switch work: without
        // this, the deserializer would hand back schema-less GenericRecord for everything.
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> realtimeKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> realtimeConsumerFactory,
            ObservationRegistry observationRegistry) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(realtimeConsumerFactory);

        // M15: hand-built factory, so Boot's spring.kafka.listener.observation-enabled property never
        // reaches it - see infra/compose/README.md's M15 section. This is what lets this consumer extract
        // the traceparent header psp-connector/payment-api produced upstream and continue THAT trace,
        // even though realtime-gateway never produces anything back to Kafka itself.
        factory.getContainerProperties().setObservationRegistry(observationRegistry);
        factory.getContainerProperties().setObservationEnabled(true);

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // No CommonErrorHandler override, no DLQ - see adapters.in.kafka.RealtimeEventListener's
        // javadoc for why (docs/diagrams/topic-map.md's explicit "analytics and realtime-gateway
        // deliberately have none" call-out). Moot on restart anyway: a fresh group.id has no
        // offsets to reset even if it mattered.
        return factory;
    }

    private static String groupId() {
        return "realtime-gateway." + resolveHostname() + "." + INSTANCE_SUFFIX;
    }

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown-host";
        }
    }
}
