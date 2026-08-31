package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.webhooknotifier.application.PlanWebhookDeliveryUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * M19's third planner listener (same {@code webhook-notifier.planner.v1} consumer group, per
 * {@code config.KafkaConsumerConfig}'s {@code refundFailedKafkaListenerContainerFactory}):
 * consumes {@code refunds.refund-failed.v1} and plans exactly one webhook delivery per event. See
 * {@link RefundCompletedListener}'s javadoc - identical shape, one topic over.
 */
@Component
public class RefundFailedListener {

    private static final Logger log = LoggerFactory.getLogger(RefundFailedListener.class);

    private final PlanWebhookDeliveryUseCase useCase;
    private final RefundFailedMapper mapper;

    public RefundFailedListener(PlanWebhookDeliveryUseCase useCase, RefundFailedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${webhook-notifier.kafka.refund-failed-topic}",
            containerFactory = "refundFailedKafkaListenerContainerFactory")
    public void onMessage(RefundFailed event, Acknowledgment ack) {
        log.info(
                "Consumed refund-failed refundId={} paymentId={} merchantId={} reason={}",
                event.getRefundId(),
                event.getPaymentId(),
                event.getMerchantId(),
                event.getReason());

        useCase.execute(mapper.toCommand(event));

        ack.acknowledge();
    }
}
