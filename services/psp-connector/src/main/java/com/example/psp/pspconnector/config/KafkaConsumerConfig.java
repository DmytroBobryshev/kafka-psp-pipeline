package com.example.psp.pspconnector.config;

import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.pspconnector.domain.exception.ProviderTimeoutException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
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
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Explicit consumer wiring for {@code payments.payment-requested.v1} - M4's headline: every
 * setting below is commented for what it does AND why this value, because that's the actual
 * learning goal of this module (see docs/PLAN.md "M4 - psp-connector: first consumer").
 *
 * <p>Values that matter for the "prove it" experiments (max.poll.records,
 * max.poll.interval.ms) are read from {@code spring.kafka.consumer.*} in {@code application.yml}
 * via {@link KafkaProperties} specifically so they can be overridden from the command line without
 * a rebuild - e.g. {@code --spring.kafka.consumer.properties.max.poll.interval.ms=5000} for the
 * rebalance-storm drill. See the README's "Prove it" section for the exact commands used.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, PaymentRequested> paymentRequestedConsumerFactory(
            KafkaProperties kafkaProperties,
            @Value("${psp-connector.schema-registry.url}") String schemaRegistryUrl) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);

        // --- group.id --------------------------------------------------------------------------
        // Identifies the consumer GROUP, not the instance. Every JVM that starts with the same
        // group.id (spring.kafka.consumer.group-id in application.yml, default
        // "psp-connector.v1" per docs/diagrams/topic-map.md's consumer-groups table) is treated
        // by the broker's group coordinator as one logical consumer that must divide the topic's
        // 12 partitions among its live members - never receive the same partition twice. This is
        // what makes the M4c partition/consumer-ratio experiment work: 12 instances sharing one
        // group.id get exactly one partition each; a 13th gets none. Set via
        // spring.kafka.consumer.group-id, read automatically by kafkaProperties above.

        // --- auto.offset.reset -------------------------------------------------------------------
        // Governs behaviour ONLY when this group.id has no committed offset yet (a brand-new
        // group, or a group whose committed offset has aged out of __consumer_offsets).
        // "earliest" replays the topic's full retained history from offset 0 on first contact;
        // "latest" (the Kafka client default) would silently skip everything already on the
        // topic. earliest is mandatory here per docs/diagrams/topic-map.md ("every group sets ...
        // auto.offset.reset=earliest") - a payment authorization service that starts fresh and
        // skips backlog is a payment authorization service that drops payments. Set via
        // spring.kafka.consumer.auto-offset-reset.

        // --- enable.auto.commit=false + manual ack ------------------------------------------------
        // The single most important correctness setting in this class. With auto-commit ON, the
        // Kafka client commits the offset of the LAST RECORD RETURNED BY poll() on a fixed timer
        // (auto.commit.interval.ms, default 5s) - regardless of whether this application has
        // actually finished processing that record yet. Kill the process between "poll returned
        // it" and "we finished handling it" and the offset may already be committed: the record
        // is gone forever from this consumer's point of view even though it was never actually
        // authorized. Manual ack flips the guarantee: we call Acknowledgment.acknowledge() (see
        // adapters.in.kafka.PaymentRequestedListener) ONLY after
        // ProcessPaymentRequestUseCase.execute() returns successfully (provider called, attempt
        // persisted, status event published). A crash before that line means the record is
        // redelivered on restart - a duplicate, never a loss. The real, measured difference
        // between these two modes is captured in the README's "duplicates vs loss" experiment.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // --- max.poll.records --------------------------------------------------------------------
        // Caps how many records ONE poll() call hands to the listener in a single batch. Kept
        // deliberately small (10, see application.yml) because processing here is synchronous and
        // can take up to psp-connector.provider.max-latency-ms (5000ms default) PER RECORD: a
        // batch of 500 (the Kafka client default) at worst-case latency is 500 x 5s = 2500s of
        // uninterrupted processing before the next poll() - which is exactly what blows
        // max.poll.interval.ms below and starts the M4b rebalance storm. Lowering max.poll.records
        // is the actual production fix for that drill, not just raising the interval (see the
        // README's "Prove it" section for both, and why only one is a real fix).

        // --- max.poll.interval.ms vs session.timeout.ms / heartbeat.interval.ms -----------------
        // These look similar and are answered by two DIFFERENT liveness checks:
        //
        //   heartbeat.interval.ms / session.timeout.ms - a BACKGROUND THREAD inside the Kafka
        //   client sends heartbeats to the group coordinator independently of whether the
        //   application's poll loop is busy. session.timeout.ms (default 45s) is how long the
        //   coordinator waits without a heartbeat before declaring this member dead;
        //   heartbeat.interval.ms (default 3s, conventionally 1/3 of session.timeout.ms) is how
        //   often that background thread sends one. This detects a genuinely DEAD or
        //   network-partitioned process - the heartbeat thread runs even while the listener
        //   method is still executing, so a slow-but-alive consumer does NOT fail this check.
        //
        //   max.poll.interval.ms (default 300000ms/5min, deliberately lowered to 5000ms for the
        //   M4b drill) is a SEPARATE, poll-loop-level check: the max wall-clock time allowed to
        //   elapse between two calls to poll(). It exists because the heartbeat thread staying
        //   alive proves the PROCESS is alive, not that it is making PROGRESS - a consumer stuck
        //   in an infinite loop (or, here, one record whose simulated provider call takes 10s
        //   against a 5s limit) would keep heartbeating forever while never returning control to
        //   poll a fresh batch. Exceeding max.poll.interval.ms makes the client proactively leave
        //   the group and trigger a rebalance, even though its heartbeats were all healthy.
        //
        // In one line: heartbeat/session answer "is the process alive?"; max.poll.interval
        // answers "is the process making progress?" - and only the second one cares how long the
        // LISTENER CODE itself takes to run. This is exactly what the M4b drill is built to make
        // visible.

        // isolation.level=read_committed - per docs/diagrams/topic-map.md, every consumer group
        // in this system sets this so a future transactional producer (the M7 ledger) never has
        // its uncommitted/aborted writes read by mistake. payment-api's M3 producer isn't
        // transactional yet, so this is a no-op today and a forward-compatible default, not a
        // requirement this module currently depends on.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // --- Deserializers: ErrorHandlingDeserializer wraps the real ones ------------------------
        // ADR-0006 category C (contract violation / poison pill - bad bytes, wrong schema): "MUST
        // be configured on every consumer factory", handled BEFORE the listener ever runs. Without
        // this wrapper, a KafkaAvroDeserializer failure throws out of poll() itself and the
        // container cannot even advance past the bad record - the classic poison-pill infinite
        // loop M8's "prove it" reproduces on purpose. ErrorHandlingDeserializer instead catches
        // the deserialization exception and hands a DeserializationException to the listener's
        // error handler (see the container factory below), which Spring Kafka classifies as
        // non-retryable by default - straight to the recoverer instead of blocking every other
        // record on this partition forever.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        // M9 Phase 1: payments.payment-requested.v1 carries the Confluent wire format (magic byte
        // + 4-byte schema id + Avro binary) - KafkaAvroDeserializer reads the schema id, fetches
        // that exact schema from Schema Registry (caching it), and decodes the binary payload
        // against it. specific.avro.reader=true is what makes it hand the listener the generated
        // com.example.psp.common.events.avro.PaymentRequested class (strongly typed, no cast)
        // instead of a schema-less GenericRecord - the task's explicit requirement, and the same
        // ergonomics psp-connector already had with JsonDeserializer's typed target class.
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentRequested>
            paymentRequestedKafkaListenerContainerFactory(
                    @Qualifier("paymentRequestedConsumerFactory")
                            ConsumerFactory<String, PaymentRequested> paymentRequestedConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentRequested> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(paymentRequestedConsumerFactory);

        // --- Spring AckMode: MANUAL_IMMEDIATE --------------------------------------------------
        // Spring Kafka's own layer on top of enable.auto.commit=false. MANUAL would let
        // acknowledge() merely mark the record as committable, actually committing later in a
        // batch alongside other acks - fewer broker round-trips, cheaper, but a slightly larger
        // redelivery window on crash. MANUAL_IMMEDIATE commits synchronously the moment
        // acknowledge() is called (adapters.in.kafka.PaymentRequestedListener, right after
        // ProcessPaymentRequestUseCase.execute() returns), trading a little throughput for the
        // tightest possible redelivery window - the right trade for a payment authorization
        // consumer, and the mode actually exercised in the duplicates-vs-loss experiment.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // --- Error handling (ADR-0006) ----------------------------------------------------------
        // FixedBackOff(1000ms, 5 attempts): a ProviderTimeoutException (category A, retryable -
        // see domain.exception.ProviderTimeoutException) gets redelivered up to 5 times, 1s apart,
        // before the DefaultErrorHandler gives up and logs. This is a deliberately partial
        // implementation: ADR-0006's real policy is a non-blocking multi-topic retry chain ending
        // in a DLQ (payments.payment-requested.v1.psp-connector.dlq per
        // docs/diagrams/topic-map.md) - that chain is explicit M8 scope, not built here. What
        // happens today after 5 failed retries is "logged and skipped", not "parked in a DLQ for
        // replay" - a real, documented gap (see README "Known issues"), not a silent one.
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1_000L, 5L));
        // Only our own classified retryable exception gets these retries; everything else keeps
        // Spring Kafka's own default classification (which already treats deserialization/
        // conversion failures - ADR-0006 category C - as non-retryable, see the comment on
        // ErrorHandlingDeserializer above).
        errorHandler.addRetryableExceptions(ProviderTimeoutException.class);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
