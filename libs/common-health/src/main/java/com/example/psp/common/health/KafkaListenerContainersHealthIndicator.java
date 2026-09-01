package com.example.psp.common.health;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

public class KafkaListenerContainersHealthIndicator implements HealthIndicator {

    private final KafkaListenerEndpointRegistry registry;

    public KafkaListenerContainersHealthIndicator(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Health health() {
        List<String> stopped = new ArrayList<>();
        Health.Builder builder = Health.up();
        int total = 0;

        for (MessageListenerContainer container : registry.getAllListenerContainers()) {
            total++;
            String id = container.getListenerId() != null ? container.getListenerId() : container.toString();
            boolean running = container.isRunning();
            boolean required = container.isAutoStartup();

            builder.withDetail(id, running ? "RUNNING" : (required ? "STOPPED" : "STOPPED (autoStartup=false)"));

            if (required && !running) {
                stopped.add(id);
            }
        }

        builder.withDetail("containers", total);

        if (!stopped.isEmpty()) {
            return builder.status("DOWN").withDetail("stoppedContainers", stopped).build();
        }
        return builder.build();
    }
}
