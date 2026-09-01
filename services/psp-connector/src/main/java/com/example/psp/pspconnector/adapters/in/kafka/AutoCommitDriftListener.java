package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.pspconnector.application.ProcessPaymentRequestUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile("auto-commit-drill")
public class AutoCommitDriftListener {

    private static final Logger log = LoggerFactory.getLogger(AutoCommitDriftListener.class);

    private final ProcessPaymentRequestUseCase useCase;
    private final PaymentRequestedMapper mapper;

    public AutoCommitDriftListener(ProcessPaymentRequestUseCase useCase, PaymentRequestedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${psp-connector.kafka.payment-requested-topic}",
            containerFactory = "autoCommitDriftKafkaListenerContainerFactory")
    public void onMessage(PaymentRequestedEvent event) {
        log.info(
                "[auto-commit-drill] Consumed payment-requested paymentId={} merchantId={}",
                event.paymentId(),
                event.merchantId());
        useCase.execute(mapper.toCommand(event));
    }
}
