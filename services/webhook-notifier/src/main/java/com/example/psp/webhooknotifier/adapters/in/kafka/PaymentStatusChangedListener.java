package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.webhooknotifier.application.PlanWebhookDeliveryUseCase;
import java.util.Set;
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
 * <p><b>Only a terminal status is planned.</b> M20 added {@code "PENDING"} (psp-connector's
 * non-terminal, pre-provider-call event); M21 added {@code "IPN_RECEIVED"}/{@code "VERIFIED"}
 * (stage 3/4 trail events, emitted before the terminal outcome). None of the three has anything to
 * tell a merchant yet, so this filters by a terminal ALLOWLIST rather than growing an ever-longer
 * skip-list of non-terminal statuses one at a time - only {@code SUCCEEDED}/{@code DECLINED}
 * becomes a planned delivery; anything else is acknowledged and dropped before {@link
 * PlanWebhookDeliveryUseCase} - which stays reused unchanged across all three planner sources (see
 * its own javadoc) - ever sees it.
 */
@Component
public class PaymentStatusChangedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusChangedListener.class);

    private static final Set<String> TERMINAL_STATUSES = Set.of("SUCCEEDED", "DECLINED");

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
        if (!TERMINAL_STATUSES.contains(event.getStatus())) {
            log.info(
                    "Skipping non-terminal payment-status-changed paymentId={} merchantId={} status={} - "
                            + "nothing to notify a merchant about yet",
                    event.getPaymentId(),
                    event.getMerchantId(),
                    event.getStatus());
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
