package com.example.psp.realtimegateway.application;

import com.example.psp.realtimegateway.domain.model.RealtimeEvent;
import com.example.psp.realtimegateway.domain.port.SseConnectionRegistry;
import org.springframework.stereotype.Service;

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
