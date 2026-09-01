package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.paymentapi.application.ApplyPaymentOutcomeUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

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
