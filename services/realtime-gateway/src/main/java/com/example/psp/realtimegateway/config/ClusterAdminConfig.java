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

@Configuration
public class ClusterAdminConfig {

    private static final int ADMIN_REQUEST_TIMEOUT_MS = 10_000;

    @Bean
    public Admin kafkaAdminClient(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildAdminProperties(null);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, ADMIN_REQUEST_TIMEOUT_MS);
        return Admin.create(props);
    }

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
