package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.webhooknotifier.application.PlanWebhookDeliveryUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * M19's second planner listener (same {@code webhook-notifier.planner.v1} consumer group as
 * {@link PaymentStatusChangedListener}, per {@code config.KafkaConsumerConfig}'s
 * {@code refundCompletedKafkaListenerContainerFactory}): consumes
 * {@code refunds.refund-completed.v1} and plans exactly one webhook delivery per event, reusing
 * {@link PlanWebhookDeliveryUseCase} completely unchanged - it has never known which business
 * event produced the {@code WebhookDeliveryCommand} it publishes, and does not need to for this
 * either.
 *
 * <p>Same scope boundary as {@link PaymentStatusChangedListener}: no DLQ for this topic (a pure
 * translation step over a source topic that already has its own consumers/DLQs elsewhere), zero
 * retries via the shared error handler, manual-immediate ack.
 */
@Component
public class RefundCompletedListener {

    private static final Logger log = LoggerFactory.getLogger(RefundCompletedListener.class);

    private final PlanWebhookDeliveryUseCase useCase;
    private final RefundCompletedMapper mapper;

    public RefundCompletedListener(PlanWebhookDeliveryUseCase useCase, RefundCompletedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${webhook-notifier.kafka.refund-completed-topic}",
            containerFactory = "refundCompletedKafkaListenerContainerFactory")
    public void onMessage(RefundCompleted event, Acknowledgment ack) {
        log.info(
                "Consumed refund-completed refundId={} paymentId={} merchantId={}",
                event.getRefundId(),
                event.getPaymentId(),
                event.getMerchantId());

        useCase.execute(mapper.toCommand(event));

        ack.acknowledge();
    }
}
