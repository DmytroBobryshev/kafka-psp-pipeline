package com.example.psp.realtimegateway.domain.port;

import com.example.psp.realtimegateway.domain.model.RealtimeEvent;
import com.example.psp.realtimegateway.domain.model.SubscriptionFilter;

public interface SseConnectionRegistry {

    String register(SubscriptionFilter filter, EventSink sink);

    void unregister(String subscriptionId);

    void broadcast(RealtimeEvent event);

    int activeSubscriptionCount();
}
