package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.FundsReserved;
import com.example.psp.paymentapi.application.RecordRefundHistoryCommand;
import com.example.psp.paymentapi.application.RecordRefundHistoryUseCase;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RefundFundsReservedListener {

    private static final Logger log = LoggerFactory.getLogger(RefundFundsReservedListener.class);

    private final RecordRefundHistoryUseCase useCase;

    public RefundFundsReservedListener(RecordRefundHistoryUseCase useCase) {
        this.useCase = useCase;
    }

    @KafkaListener(
            topics = "${payment-api.kafka.funds-reserved-topic}",
            containerFactory = "refundFundsReservedViewKafkaListenerContainerFactory")
    public void onMessage(FundsReserved event, Acknowledgment ack) {
        log.info("Consumed funds-reserved refundId={}", event.getRefundId());

        useCase.execute(
                new RecordRefundHistoryCommand(
                        UUID.fromString(event.getRefundId()),
                        UUID.fromString(event.getPaymentId()),
                        "FUNDS_RESERVED",
                        null,
                        UUID.fromString(event.getEnvelope().getEventId()),
                        event.getEnvelope().getOccurredAt()));

        ack.acknowledge();
    }
}
