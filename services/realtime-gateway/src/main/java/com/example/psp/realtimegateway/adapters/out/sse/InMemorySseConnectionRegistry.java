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

/**
 * Real (in-memory, single-instance) adapter for {@link SseConnectionRegistry} - the piece that
 * makes "connection lifecycle: register/deregister, disconnects, idle timeout, no leaks" concrete.
 *
 * <h2>Why a slow browser must never block the Kafka consumer thread</h2>
 *
 * <p>{@link #broadcast} is called synchronously from {@code adapters.in.kafka.RealtimeEventListener}'s
 * {@code @KafkaListener} method - i.e. from the ONE poll-loop thread this gateway instance uses to
 * consume all 7 subscribed topics (see {@code config.KafkaConsumerConfig}: this instance's unique
 * {@code group.id} means it owns every partition of every subscribed topic itself, so there is no
 * "someone else's consumer" to fall back on if this thread stalls). If sending to an
 * {@code SseEmitter} were done directly on that thread, ONE slow or stalled browser (a laptop
 * asleep mid-TCP-handshake, a client that stopped reading its socket) would block delivery to
 * every OTHER currently-connected browser AND, worse, delay the next {@code poll()} call - the
 * exact mechanism M4 measured causing a rebalance storm when listener processing exceeded
 * {@code max.poll.interval.ms} (services/psp-connector/README.md's M4 "Rebalance storm" section).
 * A gateway that fans events out to potentially many browsers is significantly MORE exposed to
 * this than a normal listener, since it does many downstream writes per one consumed record
 * instead of one downstream write per record.
 *
 * <p>The fix: each subscription gets its OWN single-thread {@link ExecutorService} backed by a
 * virtual thread ({@link Thread.Builder#factory()} via {@code Thread.ofVirtual()}, JEP 444, final
 * in Java 21). {@link #broadcast} only ever calls {@code dispatcher.execute(...)} - a
 * non-blocking hand-off - and returns immediately, regardless of how long the actual
 * {@code emitter.send(...)} call inside that task takes or whether it ever completes. Using a
 * SINGLE-thread executor per connection (rather than one shared pool) is deliberate: it guarantees
 * events for one browser are delivered in the order they were broadcast (a payment's timeline
 * must render in order), while virtual threads make "one executor per connection" cheap even with
 * many concurrent browsers - unlike a platform-thread-per-connection design, which would not scale
 * the same way.
 *
 * <h2>No leaks</h2>
 *
 * <p>A subscription is removed on: (1) the browser's own disconnect ({@code SseEmitter}'s
 * {@code onCompletion}/{@code onError} callbacks, wired in {@code adapters.in.web.PaymentTimelineController}),
 * (2) the {@code SseEmitter}'s configured idle timeout firing (same controller), or (3) THIS class
 * self-healing when a dispatch task's {@code sink.emit(event)} throws - a broken pipe on a client
 * that vanished without a clean TCP close is only ever discovered on the NEXT attempted write, so
 * this defensive removal is the difference between "leaked forever" and "cleaned up on the next
 * event for that filter". {@link #unregister} also shuts down that subscription's executor,
 * releasing its virtual thread.
 */
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
            // Non-blocking: does not wait for an in-flight task to finish, just stops accepting
            // new ones. Safe to call from WITHIN one of this executor's own tasks (the self-heal
            // path in broadcast() below does exactly that) - shutdown() never interrupts the
            // caller.
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
            // Hand off and return immediately - see class javadoc, "Why a slow browser must never
            // block the Kafka consumer thread". This call does NOT wait for the browser write.
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
            // Client gone (broken pipe), or the sink threw for any other reason - self-heal: this
            // subscription is dead, stop trying it again. Runs ON this subscription's own
            // dispatcher thread, which unregister() below shuts down without interrupting itself
            // (see unregister()'s javadoc).
            log.debug("SSE send failed for subscription id={}, removing", subscriptionId, ex);
            unregister(subscriptionId);
        }
    }

    private record Subscription(SubscriptionFilter filter, EventSink sink, ExecutorService dispatcher) {
    }
}
