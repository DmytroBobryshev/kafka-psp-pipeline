package com.example.psp.webhooknotifier.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.psp.common.events.avro.WebhookDeliveryRequested;
import com.example.psp.webhooknotifier.application.ReplayDlqUseCase;
import com.example.psp.webhooknotifier.domain.model.RetryHeaderNames;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("integration-test")
class RetryChainIT {

    private static final Logger log = LoggerFactory.getLogger(RetryChainIT.class);

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7"));

    static final String SCHEMA_REGISTRY_URL = "mock://webhook-notifier-it";

    static {
        KAFKA.start();
        MONGO.start();
    }

    private static final String BASE_TOPIC = "webhooks.webhook-delivery-requested.v2.retry-it";
    private static final String RETRY_5S_TOPIC = BASE_TOPIC + ".retry.5s";
    private static final String RETRY_1M_TOPIC = BASE_TOPIC + ".retry.1m";
    private static final String RETRY_15M_TOPIC = BASE_TOPIC + ".retry.15m";
    private static final String DLQ_TOPIC = BASE_TOPIC + ".dlq";
    private static final String STATUS_TOPIC = "payments.payment-status-changed.v1.retry-it";

    private static final long DELAY_TIER1_MS = 500;
    private static final long DELAY_TIER2_MS = 700;
    private static final long DELAY_TIER3_MS = 900;

    private static final String UNREACHABLE_MERCHANT_URL = "http://127.0.0.1:1";

    private static final String MERCHANT_ID = "merchant-retry-it";

    @Autowired private ReplayDlqUseCase replayDlqUseCase;

    @BeforeAll
    static void createIntegrationTopics() {
        createTopics(
                Map.of(
                        BASE_TOPIC, 1,
                        RETRY_5S_TOPIC, 1,
                        RETRY_1M_TOPIC, 1,
                        RETRY_15M_TOPIC, 1,
                        DLQ_TOPIC, 1,
                        STATUS_TOPIC, 1));
    }

    @DynamicPropertySource
    static void retryChainItProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("webhook_notifier_it"));

        registry.add("webhook-notifier.schema-registry.url", () -> SCHEMA_REGISTRY_URL);

        registry.add("webhook-notifier.kafka.payment-status-changed-topic", () -> STATUS_TOPIC);
        registry.add("webhook-notifier.kafka.delivery-requested-topic", () -> BASE_TOPIC);
        registry.add("webhook-notifier.kafka.retry-5s-topic", () -> RETRY_5S_TOPIC);
        registry.add("webhook-notifier.kafka.retry-1m-topic", () -> RETRY_1M_TOPIC);
        registry.add("webhook-notifier.kafka.retry-15m-topic", () -> RETRY_15M_TOPIC);
        registry.add("webhook-notifier.kafka.dlq-topic", () -> DLQ_TOPIC);
        registry.add("webhook-notifier.kafka.planner-group-id", () -> "webhook-notifier.planner.retry-it");
        registry.add("webhook-notifier.kafka.executor-group-id", () -> "webhook-notifier.executor.retry-it");
        registry.add("webhook-notifier.dlq-replay.consumer-group", () -> "webhook-notifier.dlq-replay.retry-it");

        registry.add("webhook-notifier.retry.delay-5s-ms", () -> DELAY_TIER1_MS);
        registry.add("webhook-notifier.retry.delay-1m-ms", () -> DELAY_TIER2_MS);
        registry.add("webhook-notifier.retry.delay-15m-ms", () -> DELAY_TIER3_MS);

        registry.add("webhook-notifier.merchant-client.base-url", () -> UNREACHABLE_MERCHANT_URL);
    }

    @Test
    void anUndeliverableWebhookHopsEveryRetryTierLandsInTheDlqAndIsReplayable() {
        UUID paymentId = UUID.randomUUID();

        try (Producer<String, Object> producer = avroProducer();
                KafkaConsumer<String, byte[]> verifier = rawConsumer()) {

            verifier.subscribe(
                    List.of(BASE_TOPIC, RETRY_5S_TOPIC, RETRY_1M_TOPIC, RETRY_15M_TOPIC, DLQ_TOPIC));

            producer.send(new ProducerRecord<>(BASE_TOPIC, MERCHANT_ID, deliveryRequested(paymentId)));
            producer.flush();

            // ---- phase 1: the chain ---------------------------------------------------------
            List<ConsumerRecord<String, byte[]>> chain =
                    drainUntil(
                            verifier,
                            Duration.ofSeconds(60),
                            records ->
                                    records.stream().anyMatch(record -> record.topic().equals(DLQ_TOPIC)));

            Map<String, List<ConsumerRecord<String, byte[]>>> byTopic =
                    chain.stream().collect(Collectors.groupingBy(ConsumerRecord::topic));
            log.info(
                    "RetryChainIT observed {}",
                    byTopic.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size())));

            assertThat(byTopic.keySet())
                    .as(
                            "the record must physically appear on every tier of the chain and then in the "
                                    + "DLQ - a blocking retry would leave the middle three empty")
                    .containsExactlyInAnyOrder(
                            BASE_TOPIC, RETRY_5S_TOPIC, RETRY_1M_TOPIC, RETRY_15M_TOPIC, DLQ_TOPIC);

            assertThat(byTopic.get(RETRY_5S_TOPIC)).hasSize(1);
            assertThat(byTopic.get(RETRY_1M_TOPIC)).hasSize(1);
            assertThat(byTopic.get(RETRY_15M_TOPIC)).hasSize(1);
            assertThat(byTopic.get(DLQ_TOPIC)).hasSize(1);

            assertThat(header(byTopic.get(RETRY_5S_TOPIC).get(0), RetryHeaderNames.ATTEMPT_COUNT))
                    .isEqualTo("2");
            assertThat(header(byTopic.get(RETRY_1M_TOPIC).get(0), RetryHeaderNames.ATTEMPT_COUNT))
                    .isEqualTo("3");
            assertThat(header(byTopic.get(RETRY_15M_TOPIC).get(0), RetryHeaderNames.ATTEMPT_COUNT))
                    .isEqualTo("4");

            ConsumerRecord<String, byte[]> dlqRecord = byTopic.get(DLQ_TOPIC).get(0);
            assertThat(dlqRecord.key()).isEqualTo(MERCHANT_ID);
            assertThat(new String(dlqRecord.value(), StandardCharsets.UTF_8))
                    .as("the DLQ payload is JSON (not Avro) so it stays readable without a schema")
                    .contains(paymentId.toString())
                    .contains(MERCHANT_ID);
            assertThat(header(dlqRecord, RetryHeaderNames.ORIGINAL_TOPIC))
                    .as("triage needs to know where the record entered the chain, not just where it died")
                    .isEqualTo(BASE_TOPIC);
            assertThat(header(dlqRecord, RetryHeaderNames.EXCEPTION_FQCN))
                    .isEqualTo("merchant-5xx-or-timeout");
            assertThat(header(dlqRecord, RetryHeaderNames.FAILED_AT)).isNotNull();

            // ---- phase 2: DLQ replay --------------------------------------------------------
            int replayed = replayUntilSomethingIsRead();
            assertThat(replayed).as("the parked record must be readable back off the DLQ").isEqualTo(1);

            List<ConsumerRecord<String, byte[]>> afterReplay =
                    drainUntil(
                            verifier,
                            Duration.ofSeconds(30),
                            records ->
                                    records.stream()
                                            .anyMatch(
                                                    record ->
                                                            record.topic().equals(BASE_TOPIC)
                                                                    && header(record, RetryHeaderNames.REPLAYED_FROM)
                                                                            != null));

            ConsumerRecord<String, byte[]> republished =
                    afterReplay.stream()
                            .filter(
                                    record ->
                                            record.topic().equals(BASE_TOPIC)
                                                    && header(record, RetryHeaderNames.REPLAYED_FROM) != null)
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "ReplayDlqUseCase did not republish to " + BASE_TOPIC));

            assertThat(header(republished, RetryHeaderNames.REPLAYED_FROM))
                    .as("a replayed record must say where it came from - otherwise a replay loop is invisible")
                    .isEqualTo(DLQ_TOPIC);
            assertThat(header(republished, RetryHeaderNames.REPLAY_COUNT)).isEqualTo("1");
            assertThat(republished.key()).isEqualTo(MERCHANT_ID);
        }
    }

    private int replayUntilSomethingIsRead() {
        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            int replayed = replayDlqUseCase.replay(10);
            if (replayed > 0) {
                return replayed;
            }
        }
        return 0;
    }

    private static WebhookDeliveryRequested deliveryRequested(UUID paymentId) {
        return WebhookDeliveryRequested.newBuilder()
                .setPaymentId(paymentId.toString())
                .setMerchantId(MERCHANT_ID)
                // scale 4 - 05-webhook-delivery-requested.avsc declares decimal(19,4).
                .setAmount(new BigDecimal("12.3400"))
                .setCurrency("EUR")
                .setStatus("SUCCEEDED")
                .setDeclineReason(null)
                .setCausationEventId(UUID.randomUUID().toString())
                .setTraceId(UUID.randomUUID().toString().replace("-", ""))
                .setCorrelationId(UUID.randomUUID().toString())
                .build();
    }

    private static Producer<String, Object> avroProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, byte[]> rawConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "webhook-retry-it-verifier-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    private static List<ConsumerRecord<String, byte[]>> drainUntil(
            KafkaConsumer<String, byte[]> consumer,
            Duration timeout,
            Predicate<List<ConsumerRecord<String, byte[]>>> done) {
        List<ConsumerRecord<String, byte[]>> collected = new ArrayList<>();
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, byte[]> polled = consumer.poll(Duration.ofMillis(250));
            polled.forEach(collected::add);
            if (done.test(collected)) {
                return collected;
            }
        }
        return collected;
    }

    private static String header(ConsumerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static void createTopics(Map<String, Integer> topicToPartitions) {
        Map<String, Object> props =
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            List<NewTopic> topics = new ArrayList<>();
            topicToPartitions.forEach(
                    (topic, partitions) -> topics.add(new NewTopic(topic, partitions, (short) 1)));
            admin.createTopics(topics).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while creating topics", e);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("failed to create topics " + topicToPartitions, e);
            }
        }
    }
}
