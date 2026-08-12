package com.example.psp.common.health;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

/**
 * Reports the Kafka Streams client's own {@link KafkaStreams.State}.
 *
 * <p>Only analytics has a Streams topology, and for it "ready" is a stronger claim than "the
 * listener containers are running": a Streams application that is {@code REBALANCING} or
 * restoring state from its changelog is alive but is not yet answering interactive queries
 * correctly, and {@code ERROR}/{@code NOT_RUNNING} means the topology died while the HTTP port
 * stayed open - exactly the M15 failure mode.
 *
 * <p>{@code REBALANCING} is treated as NOT ready but is not an error: during a rolling update
 * that is precisely the window in which the pod should be out of the Service's endpoints.
 * {@code CREATED} is also not-ready, because the topology has not started consuming yet.
 */
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
