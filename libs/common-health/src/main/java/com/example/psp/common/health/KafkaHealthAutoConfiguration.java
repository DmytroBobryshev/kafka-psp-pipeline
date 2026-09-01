package com.example.psp.common.health;

import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@AutoConfiguration(after = KafkaAutoConfiguration.class)
public class KafkaHealthAutoConfiguration {

    @Bean("kafkaListeners")
    @ConditionalOnProperty(name = "psp.health.kafka.enabled", havingValue = "true")
    public HealthIndicator kafkaListenersHealthIndicator(KafkaListenerEndpointRegistry registry) {
        return new KafkaListenerContainersHealthIndicator(registry);
    }

    @Bean("kafkaStreams")
    @ConditionalOnProperty(name = "psp.health.kafka.streams.enabled", havingValue = "true")
    public HealthIndicator kafkaStreamsHealthIndicator(StreamsBuilderFactoryBean factoryBean) {
        return new KafkaStreamsHealthIndicator(factoryBean);
    }
}
