package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.pspconnector.application.ProcessPaymentRequestUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Profile("!auto-commit-drill")
public class PaymentRequestedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentRequestedListener.class);

    private final ProcessPaymentRequestUseCase useCase;
    private final PaymentRequestedMapper mapper;

    public PaymentRequestedListener(ProcessPaymentRequestUseCase useCase, PaymentRequestedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${psp-connector.kafka.payment-requested-topic}",
            containerFactory = "paymentRequestedKafkaListenerContainerFactory")
    public void onMessage(PaymentRequested event, Acknowledgment ack) {
        log.info(
                "Consumed payment-requested paymentId={} merchantId={}",
                event.getPaymentId(),
                event.getMerchantId());

        useCase.execute(mapper.toCommand(event));

        ack.acknowledge();
    }
}
