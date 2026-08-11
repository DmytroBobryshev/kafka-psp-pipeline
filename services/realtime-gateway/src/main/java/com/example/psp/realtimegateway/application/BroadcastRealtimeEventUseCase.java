package com.example.psp.realtimegateway.application;

import com.example.psp.realtimegateway.domain.model.RealtimeEvent;
import com.example.psp.realtimegateway.domain.port.SseConnectionRegistry;
import org.springframework.stereotype.Service;

/**
 * Called once per consumed Kafka record by {@code adapters.in.kafka.RealtimeEventListener}, after
 * it has been mapped from Avro to the transport-agnostic {@link RealtimeEvent}. Deliberately
 * thin - the interesting decision (never block the calling thread) lives in the
 * {@link SseConnectionRegistry} implementation, not here.
 */
@Service
public class BroadcastRealtimeEventUseCase {

    private final SseConnectionRegistry registry;

    public BroadcastRealtimeEventUseCase(SseConnectionRegistry registry) {
        this.registry = registry;
    }

    public void execute(RealtimeEvent event) {
        registry.broadcast(event);
    }
}
