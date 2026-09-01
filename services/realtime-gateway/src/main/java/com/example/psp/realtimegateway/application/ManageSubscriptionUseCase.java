package com.example.psp.realtimegateway.application;

import com.example.psp.realtimegateway.domain.port.EventSink;
import com.example.psp.realtimegateway.domain.port.SseConnectionRegistry;
import com.example.psp.realtimegateway.domain.model.SubscriptionFilter;
import org.springframework.stereotype.Service;

@Service
public class ManageSubscriptionUseCase {

    private final SseConnectionRegistry registry;

    public ManageSubscriptionUseCase(SseConnectionRegistry registry) {
        this.registry = registry;
    }

    public String subscribe(SubscriptionFilter filter, EventSink sink) {
        return registry.register(filter, sink);
    }

    public void unsubscribe(String subscriptionId) {
        registry.unregister(subscriptionId);
    }
}
