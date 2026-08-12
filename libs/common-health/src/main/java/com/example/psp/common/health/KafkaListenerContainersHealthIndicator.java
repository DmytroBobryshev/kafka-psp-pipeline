package com.example.psp.common.health;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * Reports DOWN when a {@code @KafkaListener} container that is supposed to be running is not.
 *
 * <p><b>Why this exists.</b> M15 established that a service can answer {@code GET
 * /actuator/health} with {@code UP} while every one of its listener containers is stopped: the
 * servlet container is a different thing from the consumer threads, and Spring Boot's built-in
 * {@code readinessState} only knows about the former. On Docker Compose that produced a
 * misleading dashboard. On Kubernetes it produces something worse - a pod that passes its
 * readiness probe, stays in its Service's endpoint list, and during a rolling update lets the
 * previous ReplicaSet be scaled down in favour of a replacement that consumes nothing.
 *
 * <p><b>What "supposed to be running" means.</b> Only containers with {@code autoStartup=true}
 * are required to be running. A container that was deliberately created stopped (Spring Kafka's
 * {@code autoStartup=false}, used here for the DLQ-replay listener that is started on demand)
 * is reported but never fails the check - otherwise the indicator would punish correct design.
 *
 * <p>Every container is listed in the health details with its state, so {@code kubectl exec ...
 * curl localhost:PORT/actuator/health} names the listener that died rather than just saying
 * DOWN.
 */
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
        // A service that declares no listeners at all (api-gateway) never reaches here: the bean
        // is only created when a KafkaListenerEndpointRegistry exists.
        return builder.build();
    }
}
