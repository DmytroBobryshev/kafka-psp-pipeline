package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
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
 *
 * <p><b>PENDING is filtered here, not planned.</b> {@code status = "PENDING"} is psp-connector's
 * non-terminal, pre-provider-call event - there is nothing to tell a merchant yet. Only a terminal
 * {@code SUCCEEDED}/{@code DECLINED} becomes a planned delivery; a PENDING record is acknowledged
 * and dropped before {@link PlanWebhookDeliveryUseCase} - which stays reused unchanged across all
 * three planner sources (see its own javadoc) - ever sees it.
 */
@Component
public class PaymentStatusChangedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusChangedListener.class);

    private static final String PENDING_STATUS = "PENDING";

    private final PlanWebhookDeliveryUseCase useCase;
    private final PaymentStatusChangedMapper mapper;

    public PaymentStatusChangedListener(PlanWebhookDeliveryUseCase useCase, PaymentStatusChangedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${webhook-notifier.kafka.payment-status-changed-topic}",
            containerFactory = "plannerKafkaListenerContainerFactory")
    public void onMessage(PaymentStatusChanged event, Acknowledgment ack) {
        if (PENDING_STATUS.equals(event.getStatus())) {
            log.info(
                    "Skipping non-terminal payment-status-changed paymentId={} merchantId={} status=PENDING - "
                            + "nothing to notify a merchant about yet",
                    event.getPaymentId(),
                    event.getMerchantId());
            ack.acknowledge();
            return;
        }

        log.info(
                "Consumed payment-status-changed paymentId={} merchantId={} status={}",
                event.getPaymentId(),
                event.getMerchantId(),
                event.getStatus());

        useCase.execute(mapper.toCommand(event));

        ack.acknowledge();
    }
}
