package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.webhooknotifier.application.PlanWebhookDeliveryUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * The planner listener (topic-map's {@code webhook-notifier.planner.v1} group): consumes
 * {@code payments.payment-status-changed.v1} using the container factory built in
 * {@code config.KafkaConsumerConfig#plannerKafkaListenerContainerFactory}.
 *
 * <p>No DLQ for THIS topic (see docs/diagrams/topic-map.md's "Dead-letter topics for other
 * consumers" section - webhook-notifier's only provisioned DLQ is
 * {@code webhooks.webhook-delivery-requested.v1.dlq}, downstream of the planner, not this
 * listener). {@code ErrorHandlingDeserializer} is still configured on this consumer factory
 * (ADR-0006 mandates it on every consumer factory, and it is the fix M8's poison-pill "prove it"
 * experiment demonstrates) - a bad record here is logged and the offset is skipped rather than
 * blocking the partition forever, but it is not parked anywhere for replay. A documented scope
 * boundary, not a silent one: this is a pure translation step with nothing of its own to protect
 * against loss (the source of truth, {@code payments.payment-status-changed.v1}, is retained for
 * 7 days and already has its own consumers/DLQs elsewhere in the system).
 */
@Component
public class PaymentStatusChangedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusChangedListener.class);

    private final PlanWebhookDeliveryUseCase useCase;
    private final PaymentStatusChangedMapper mapper;

    public PaymentStatusChangedListener(PlanWebhookDeliveryUseCase useCase, PaymentStatusChangedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${webhook-notifier.kafka.payment-status-changed-topic}",
            containerFactory = "plannerKafkaListenerContainerFactory")
    public void onMessage(PaymentStatusChangedEvent event, Acknowledgment ack) {
        log.info(
                "Consumed payment-status-changed paymentId={} merchantId={} status={}",
                event.paymentId(),
                event.merchantId(),
                event.status());

        useCase.execute(mapper.toCommand(event));

        ack.acknowledge();
    }
}
