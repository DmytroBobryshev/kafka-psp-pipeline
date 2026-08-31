package com.example.psp.realtimegateway.config;

import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * M17's cluster-ops beans: an {@link Admin} AdminClient for
 * {@code adapters.out.kafka.KafkaClusterInspector} (topics/groups/lag - page 5) and a
 * byte-array-only {@link ConsumerFactory} for {@code adapters.out.kafka.KafkaDlqBrowser} (the DLQ
 * peek - page 3).
 *
 * <p>Both are built from {@link KafkaProperties#buildAdminProperties} /
 * {@link KafkaProperties#buildConsumerProperties} - the SAME merged
 * {@code spring.kafka.bootstrap-servers} + {@code spring.kafka.properties} (SASL/SCRAM-SHA-512 in
 * the {@code docker-compose}/k8s profiles) that {@code config.KafkaConsumerConfig}'s SSE consumer
 * factory already uses. That merge is exactly why the {@code docker-compose} profile's javadoc
 * calls {@code spring.kafka.properties} "the COMMON block" - one place authenticates the producer,
 * the consumer AND the admin client.
 */
@Configuration
public class ClusterAdminConfig {

    /**
     * Bounds every AdminClient request issued through this bean - {@code listTopics},
     * {@code describeTopics}, {@code listConsumerGroups}, {@code describeConsumerGroups},
     * {@code listConsumerGroupOffsets}, {@code listOffsets} all inherit this as their default
     * request timeout, so none of them can hang the calling HTTP thread indefinitely if the
     * cluster is unreachable.
     */
    private static final int ADMIN_REQUEST_TIMEOUT_MS = 10_000;

    /**
     * Spring infers the destroy method from {@link Admin#close()} (an {@code AutoCloseable} with a
     * public no-arg {@code close}), so this client is closed automatically on context shutdown -
     * no explicit {@code destroyMethod} needed.
     */
    @Bean
    public Admin kafkaAdminClient(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildAdminProperties(null);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, ADMIN_REQUEST_TIMEOUT_MS);
        return Admin.create(props);
    }

    /**
     * A separate {@link ConsumerFactory}, NOT the one {@code config.KafkaConsumerConfig} builds
     * for the SSE listener: that one deserializes Avro via {@code KafkaAvroDeserializer} and
     * carries a per-instance unique {@code group.id}. The DLQ peek needs neither - its values are
     * Avro on some DLQs and arbitrary bytes on others (never Avro-decoded here, see
     * {@code domain.model.DlqRecordView}'s javadoc), and {@code adapters.out.kafka.KafkaDlqBrowser}
     * uses {@code assign()}, not {@code subscribe()}, so it never joins a consumer group at all -
     * {@code group.id} is left unset on purpose (see that class's javadoc for why that keeps this
     * endpoint off the consumer-group ACL entirely).
     */
    @Bean("dlqPeekConsumerFactory")
    public ConsumerFactory<byte[], byte[]> dlqPeekConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }
}
