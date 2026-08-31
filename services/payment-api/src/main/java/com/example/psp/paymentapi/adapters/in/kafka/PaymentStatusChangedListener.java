package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.paymentapi.application.ApplyPaymentOutcomeUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * M19: payment-api's first-ever consumer of {@code payments.payment-status-changed.v1} - group
 * {@code payment-api.status-view.v1} ({@code config.PaymentStatusViewKafkaConfig}), the same event
 * ledger and webhook-notifier already consume, kept as a third independent local projection
 * (ADR-0005) so the transactions panel can answer {@code GET /api/payments} without a
 * cross-service call ADR-0004 would forbid anyway.
 *
 * <p>No DLQ for this topic - the same documented scope boundary
 * {@code webhook-notifier.adapters.in.kafka.PaymentStatusChangedListener} already carries for the
 * identical topic (see that class's javadoc): this is a derived, lossy read-model listener
 * (ADR-0006's "not every consumer gets a DLQ" list - {@code analytics}/{@code realtime-gateway}
 * are the other two), not a saga participant with irreplaceable state. A record this listener
 * cannot process is logged and skipped by the container's error handler
 * ({@code config.PaymentStatusViewKafkaConfig}); the source topic itself is retained for 7 days
 * and already has its own DLQ'd consumers elsewhere in the system.
 *
 * <p>{@code ErrorHandlingDeserializer} is still configured on this consumer factory regardless
 * (ADR-0006 mandates it on every consumer factory, no exceptions) - a poison-pill record is caught
 * before this listener ever runs, not left to jam the partition forever (M8's "prove it").
 *
 * <p>Manual, immediate acknowledgment - same convention as every other listener in this system
 * that does not (yet) need the "hold the ack open until a downstream hand-off completes" shape
 * {@code webhook-notifier}'s executor listener uses: {@link ApplyPaymentOutcomeUseCase#execute} is
 * a single synchronous, idempotent DB write, so acking immediately after it returns is both
 * correct and simple.
 */
@Component
public class PaymentStatusChangedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusChangedListener.class);

    private final ApplyPaymentOutcomeUseCase useCase;
    private final PaymentStatusChangedMapper mapper;

    public PaymentStatusChangedListener(ApplyPaymentOutcomeUseCase useCase, PaymentStatusChangedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${payment-api.kafka.payment-status-changed-topic}",
            containerFactory = "paymentStatusViewKafkaListenerContainerFactory")
    public void onMessage(PaymentStatusChanged event, Acknowledgment ack) {
        log.info(
                "Consumed payment-status-changed paymentId={} status={}",
                event.getPaymentId(),
                event.getStatus());

        useCase.execute(mapper.toCommand(event));

        ack.acknowledge();
    }
}
