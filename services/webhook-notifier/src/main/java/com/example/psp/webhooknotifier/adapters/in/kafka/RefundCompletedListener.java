package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.webhooknotifier.application.PlanWebhookDeliveryUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

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
