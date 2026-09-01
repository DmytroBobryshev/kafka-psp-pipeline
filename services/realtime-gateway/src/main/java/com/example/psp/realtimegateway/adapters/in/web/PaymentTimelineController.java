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
