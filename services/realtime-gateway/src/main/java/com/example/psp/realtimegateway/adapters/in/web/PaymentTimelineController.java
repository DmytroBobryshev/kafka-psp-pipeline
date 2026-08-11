package com.example.psp.realtimegateway.adapters.in.web;

import com.example.psp.realtimegateway.application.ManageSubscriptionUseCase;
import com.example.psp.realtimegateway.domain.model.SubscriptionFilter;
import com.example.psp.realtimegateway.domain.port.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * The SSE edge (module brief: "pushes them to browsers over SSE, filtered by paymentId and/or
 * merchantId so a browser can watch one payment's timeline"). This is the ONLY class in this
 * service that imports {@code SseEmitter} - {@code domain/} and {@code application/} know only
 * the framework-free {@link EventSink} (ADR-0007).
 *
 * <p>Per ADR-0004 ("Push to the browser. realtime-gateway consumes payments.* / refunds.* and
 * pushes SSE to the UI. It never queries another service.") this is edge traffic, not
 * service-to-service communication - the same category api-gateway (M16) will eventually front,
 * exactly like payment-api's and analytics' REST endpoints today, pre-gateway.
 *
 * <h2>Connection lifecycle</h2>
 *
 * <ul>
 *   <li><b>Register</b> - {@link #stream} builds an {@code SseEmitter}, wraps it as an
 *       {@link EventSink}, and subscribes via {@link ManageSubscriptionUseCase}.
 *   <li><b>Client disconnect</b> - a clean browser-side close (tab closed, navigated away) fires
 *       {@code onCompletion}; a broken connection (network drop, proxy timeout) fires
 *       {@code onError}. Both unregister the subscription so it is never left registered after the
 *       browser is gone - see {@code adapters.out.sse.InMemorySseConnectionRegistry}'s javadoc for
 *       the belt-and-braces self-healing that also catches sends to a connection that died between
 *       events, before either callback had a chance to fire.
 *   <li><b>Idle timeout</b> - {@code SseEmitter}'s own constructor timeout
 *       ({@code realtime-gateway.sse.timeout-ms}) closes a connection that neither sent data nor
 *       received any for that long, so a silently-vanished client (laptop closed, network dropped
 *       without a clean FIN) cannot leak a subscription forever even if it never triggers
 *       {@code onError}.
 * </ul>
 */
@RestController
@RequestMapping("/api/realtime")
public class PaymentTimelineController {

    private static final Logger log = LoggerFactory.getLogger(PaymentTimelineController.class);

    private final ManageSubscriptionUseCase subscriptionUseCase;
    private final long emitterTimeoutMs;

    public PaymentTimelineController(
            ManageSubscriptionUseCase subscriptionUseCase,
            @Value("${realtime-gateway.sse.timeout-ms}") long emitterTimeoutMs) {
        this.subscriptionUseCase = subscriptionUseCase;
        this.emitterTimeoutMs = emitterTimeoutMs;
    }

    @GetMapping(path = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(
            @RequestParam(name = "paymentId", required = false) String paymentId,
            @RequestParam(name = "merchantId", required = false) String merchantId) {
        if (paymentId == null && merchantId == null) {
            throw new IllegalArgumentException(
                    "At least one of paymentId or merchantId query parameters is required");
        }

        SubscriptionFilter filter = new SubscriptionFilter(paymentId, merchantId);
        SseEmitter emitter = new SseEmitter(emitterTimeoutMs);

        EventSink sink =
                event ->
                        emitter.send(
                                SseEmitter.event().id(event.eventId()).name(event.eventType()).data(event));

        String subscriptionId = subscriptionUseCase.subscribe(filter, sink);
        log.info(
                "SSE connection opened id={} paymentId={} merchantId={} timeoutMs={}",
                subscriptionId,
                paymentId,
                merchantId,
                emitterTimeoutMs);

        emitter.onCompletion(
                () -> {
                    log.info("SSE connection completed id={}", subscriptionId);
                    subscriptionUseCase.unsubscribe(subscriptionId);
                });
        emitter.onTimeout(
                () -> {
                    log.info("SSE connection idle-timed-out id={}", subscriptionId);
                    subscriptionUseCase.unsubscribe(subscriptionId);
                    emitter.complete();
                });
        emitter.onError(
                ex -> {
                    log.info("SSE connection errored id={} - {}", subscriptionId, ex.toString());
                    subscriptionUseCase.unsubscribe(subscriptionId);
                });

        return emitter;
    }
}
