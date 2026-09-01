package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.common.events.avro.FundsReserved;
import com.example.psp.pspconnector.application.ExecuteRefundUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class FundsReservedListener {

    private static final Logger log = LoggerFactory.getLogger(FundsReservedListener.class);

    private final ExecuteRefundUseCase useCase;
    private final FundsReservedMapper mapper;

    public FundsReservedListener(ExecuteRefundUseCase useCase, FundsReservedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${psp-connector.kafka.funds-reserved-topic}",
            containerFactory = "fundsReservedKafkaListenerContainerFactory")
    public void onMessage(FundsReserved event, Acknowledgment ack) {
        log.info(
                "Consumed funds-reserved refundId={} paymentId={} merchantId={} amount={}",
                event.getRefundId(),
                event.getPaymentId(),
                event.getMerchantId(),
                event.getAmount());

        useCase.execute(mapper.toCommand(event));

        ack.acknowledge();
    }
}
