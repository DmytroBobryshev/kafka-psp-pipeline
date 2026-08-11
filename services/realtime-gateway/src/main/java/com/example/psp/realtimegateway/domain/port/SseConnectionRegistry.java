package com.example.psp.realtimegateway.domain.port;

import com.example.psp.realtimegateway.domain.model.RealtimeEvent;
import com.example.psp.realtimegateway.domain.model.SubscriptionFilter;

/**
 * Outbound port for the connection registry that holds every currently-connected browser's
 * {@link SubscriptionFilter} and {@link EventSink}. Implemented by
 * {@code adapters.out.sse.InMemorySseConnectionRegistry} - the domain never imports
 * {@code SseEmitter} or any servlet type (ADR-0007), only this interface and the plain
 * {@link EventSink} callback.
 *
 * <p>This is the port that carries the module's central lesson in its contract: {@link #broadcast}
 * is called once per Kafka record, from the Kafka consumer thread, for EVERY currently-registered
 * subscription regardless of which gateway instance originally "should" have received that
 * partition under a shared consumer group - see {@code config.KafkaConsumerConfig}'s javadoc for
 * why every instance must see every event in the first place.
 */
public interface SseConnectionRegistry {

    /**
     * Registers a new connection and returns its subscription id, used later to unregister it.
     */
    String register(SubscriptionFilter filter, EventSink sink);

    /** Idempotent: unregistering a subscription id that is already gone (or never existed) is a no-op. */
    void unregister(String subscriptionId);

    /**
     * Delivers {@code event} to every subscription whose filter matches it. MUST NOT perform
     * blocking I/O on the calling thread - see the implementation's javadoc for why (that thread
     * is, in production, the single Kafka consumer poll-loop thread for this whole gateway
     * instance).
     */
    void broadcast(RealtimeEvent event);

    /** Number of currently-registered subscriptions - exposed for the health/debug endpoint. */
    int activeSubscriptionCount();
}
