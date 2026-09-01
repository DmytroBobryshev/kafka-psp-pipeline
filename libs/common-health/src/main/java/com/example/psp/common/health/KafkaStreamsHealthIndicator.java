package com.example.psp.common.health;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

public class KafkaStreamsHealthIndicator implements HealthIndicator {

    private final StreamsBuilderFactoryBean factoryBean;

    public KafkaStreamsHealthIndicator(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    @Override
    public Health health() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            return Health.down().withDetail("state", "NOT_STARTED").build();
        }
        KafkaStreams.State state = streams.state();
        Health.Builder builder = state == KafkaStreams.State.RUNNING ? Health.up() : Health.down();
        return builder.withDetail("state", state.name())
                .withDetail("applicationId", applicationId())
                .build();
    }

    private String applicationId() {
        try {
            return factoryBean.getStreamsConfiguration() == null
                    ? "unknown"
                    : String.valueOf(factoryBean.getStreamsConfiguration().get("application.id"));
        } catch (RuntimeException e) {
            return "unknown";
        }
    }
}
