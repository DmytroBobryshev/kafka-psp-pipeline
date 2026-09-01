package com.example.psp.realtimegateway.adapters.out.sse;

import com.example.psp.realtimegateway.domain.model.RealtimeEvent;
import com.example.psp.realtimegateway.domain.model.SubscriptionFilter;
import com.example.psp.realtimegateway.domain.port.EventSink;
import com.example.psp.realtimegateway.domain.port.SseConnectionRegistry;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class InMemorySseConnectionRegistry implements SseConnectionRegistry {

    private static final Logger log = LoggerFactory.getLogger(InMemorySseConnectionRegistry.class);

    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();

    @Override
    public String register(SubscriptionFilter filter, EventSink sink) {
        String subscriptionId = UUID.randomUUID().toString();
        ExecutorService dispatcher =
                Executors.newSingleThreadExecutor(Thread.ofVirtual().name("sse-dispatch-" + subscriptionId).factory());
        subscriptions.put(subscriptionId, new Subscription(filter, sink, dispatcher));
        log.info(
                "Registered SSE subscription id={} paymentId={} merchantId={} activeSubscriptions={}",
                subscriptionId,
                filter.paymentId(),
                filter.merchantId(),
                subscriptions.size());
        return subscriptionId;
    }

    @Override
    public void unregister(String subscriptionId) {
        Subscription subscription = subscriptions.remove(subscriptionId);
        if (subscription != null) {
            subscription.dispatcher().shutdown();
            log.info(
                    "Unregistered SSE subscription id={} activeSubscriptions={}",
                    subscriptionId,
                    subscriptions.size());
        }
    }

    @Override
    public void broadcast(RealtimeEvent event) {
        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            String subscriptionId = entry.getKey();
            Subscription subscription = entry.getValue();
            if (!subscription.filter().matches(event)) {
                continue;
            }
            subscription.dispatcher().execute(() -> deliver(subscriptionId, subscription, event));
        }
    }

    @Override
    public int activeSubscriptionCount() {
        return subscriptions.size();
    }

    private void deliver(String subscriptionId, Subscription subscription, RealtimeEvent event) {
        try {
            subscription.sink().emit(event);
        } catch (Exception ex) {
            log.debug("SSE send failed for subscription id={}, removing", subscriptionId, ex);
            unregister(subscriptionId);
        }
    }

    private record Subscription(SubscriptionFilter filter, EventSink sink, ExecutorService dispatcher) {
    }
}
