package com.example.psp.realtimegateway.application;

import com.example.psp.realtimegateway.domain.port.EventSink;
import com.example.psp.realtimegateway.domain.port.SseConnectionRegistry;
import com.example.psp.realtimegateway.domain.model.SubscriptionFilter;
import org.springframework.stereotype.Service;

/**
 * Connection-lifecycle use case: register/deregister a browser's subscription.
 * {@code application/} orchestrates the {@link SseConnectionRegistry} port only (ADR-0007) - it
 * never imports {@code SseEmitter}, which is why {@code adapters.in.web.PaymentTimelineController}
 * builds the emitter and its lifecycle callbacks itself and only hands this class the resulting
 * {@link EventSink}.
 */
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
