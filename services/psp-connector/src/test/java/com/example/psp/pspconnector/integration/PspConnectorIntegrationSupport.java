package com.example.psp.pspconnector.integration;

import com.example.psp.common.events.avro.EventEnvelope;
import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@ActiveProfiles("integration-test")
abstract class PspConnectorIntegrationSupport {

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"))
                    .withDatabaseName("psp_connector")
                    .withUsername("psp_connector")
                    .withPassword("psp_connector");

    static final String SCHEMA_REGISTRY_URL = "mock://psp-connector-it";

    static {
        KAFKA.start();
        POSTGRES.start();
    }

    @Autowired protected KafkaListenerEndpointRegistry listenerEndpointRegistry;

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);

        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.flyway.enabled", () -> true);

        registry.add("psp-connector.schema-registry.url", () -> SCHEMA_REGISTRY_URL);

        registry.add("psp-connector.provider.forced-outcome", () -> "APPROVED");
        registry.add("psp-connector.provider.duplicate-rate", () -> "0.0");
    }

    protected static void createTopics(Map<String, Integer> topicToPartitions) {
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

    protected static Producer<String, Object> avroProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        return new KafkaProducer<>(props);
    }

    protected static PaymentRequested paymentRequested(
            UUID eventId, UUID paymentId, String merchantId, BigDecimal amount) {
        EventEnvelope envelope =
                EventEnvelope.newBuilder()
                        .setEventId(eventId.toString())
                        .setEventType("payments.payment-requested.v1")
                        .setEventVersion(1)
                        .setAggregateId(paymentId.toString())
                        .setAggregateType("payment")
                        .setOccurredAt(Instant.now())
                        .setSource("psp-connector-it")
                        .setTraceId(eventId.toString().replace("-", "").substring(0, 32))
                        .setCorrelationId(eventId.toString())
                        .setCausationId(null)
                        .build();
        return PaymentRequested.newBuilder()
                .setEnvelope(envelope)
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId)
                .setAmount(amount.setScale(4))
                .setCurrency("EUR")
                .setStatus("PENDING")
                .build();
    }

    protected static KafkaConsumer<String, PaymentStatusChanged> statusChangedConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "psp-connector-it-verifier-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new KafkaConsumer<>(props);
    }

    protected static <V> List<ConsumerRecord<String, V>> drainUntil(
            KafkaConsumer<String, V> consumer,
            String topic,
            Duration timeout,
            Predicate<List<ConsumerRecord<String, V>>> done) {
        consumer.subscribe(List.of(topic));
        List<ConsumerRecord<String, V>> collected = new ArrayList<>();
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, V> polled = consumer.poll(Duration.ofMillis(250));
            polled.forEach(collected::add);
            if (done.test(collected)) {
                return collected;
            }
        }
        return collected;
    }

    protected MessageListenerContainer listenerContainerFor(String topic) {
        return listenerEndpointRegistry.getListenerContainers().stream()
                .filter(
                        container -> {
                            String[] topics = container.getContainerProperties().getTopics();
                            return topics != null && Arrays.asList(topics).contains(topic);
                        })
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "no @KafkaListener container is subscribed to " + topic));
    }

    protected static void send(Producer<String, Object> producer, String topic, String key, Object value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    protected static final Set<String> TERMINAL_STATUSES = Set.of("SUCCEEDED", "DECLINED");

    protected static List<ConsumerRecord<String, PaymentStatusChanged>> terminalOnly(
            List<ConsumerRecord<String, PaymentStatusChanged>> records) {
        return records.stream().filter(r -> TERMINAL_STATUSES.contains(r.value().getStatus())).toList();
    }
}
