package com.example.psp.realtimegateway.domain.port;

import com.example.psp.realtimegateway.domain.model.RealtimeEvent;

/**
 * A callback the domain invokes to deliver one event to whoever subscribed - implemented by
 * {@code adapters.in.web.PaymentTimelineController} as a thin wrapper around a Spring
 * {@code SseEmitter}, without {@code domain/} ever importing that Spring MVC type (ADR-0007).
 *
 * <p>{@code throws Exception} deliberately: {@code SseEmitter#send} throws {@code IOException}
 * when the browser is gone (broken pipe, closed tab). {@code adapters.out.sse.InMemorySseConnectionRegistry}
 * is the only caller and treats any exception here as "this connection is dead, remove it" -
 * never as a reason to fail the Kafka consumer thread that triggered the broadcast (see that
 * class's javadoc for why that distinction matters).
 */
@FunctionalInterface
public interface EventSink {

    void emit(RealtimeEvent event) throws Exception;
}
