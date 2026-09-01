package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.webhooknotifier.application.PlanWebhookDeliveryUseCase;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class PaymentStatusChangedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusChangedListener.class);

    private static final Set<String> TERMINAL_STATUSES = Set.of("SUCCEEDED", "DECLINED", "EXPIRED");

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
