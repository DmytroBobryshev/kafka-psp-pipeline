package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundStatusChanged;
import com.example.psp.webhooknotifier.application.PlanWebhookDeliveryUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RefundExpiredListener {

    private static final Logger log = LoggerFactory.getLogger(RefundExpiredListener.class);
    private static final String EXPIRED_STATUS = "EXPIRED";

    private final PlanWebhookDeliveryUseCase useCase;
    private final RefundExpiredMapper mapper;

    public RefundExpiredListener(PlanWebhookDeliveryUseCase useCase, RefundExpiredMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${webhook-notifier.kafka.refund-status-changed-topic}",
            containerFactory = "refundStatusChangedKafkaListenerContainerFactory")
    public void onMessage(RefundStatusChanged event, Acknowledgment ack) {
        if (!EXPIRED_STATUS.equals(event.getStatus())) {
            // PENDING/IPN_RECEIVED/VERIFIED - not a merchant-facing outcome, see class javadoc.
            log.debug(
                    "Skipping refund-status-changed refundId={} status={} - not a webhook trigger",
                    event.getRefundId(),
                    event.getStatus());
            ack.acknowledge();
            return;
        }

        log.info(
                "Consumed refund-status-changed EXPIRED refundId={} paymentId={} merchantId={}",
                event.getRefundId(),
                event.getPaymentId(),
                event.getMerchantId());

        useCase.execute(mapper.toCommand(event));

        ack.acknowledge();
    }
}
