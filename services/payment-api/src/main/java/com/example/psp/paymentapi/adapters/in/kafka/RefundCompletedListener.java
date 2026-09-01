package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.paymentapi.application.RecordRefundHistoryCommand;
import com.example.psp.paymentapi.application.RecordRefundHistoryUseCase;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RefundCompletedListener {

    private static final Logger log = LoggerFactory.getLogger(RefundCompletedListener.class);

    private final RecordRefundHistoryUseCase useCase;

    public RefundCompletedListener(RecordRefundHistoryUseCase useCase) {
        this.useCase = useCase;
    }

    @KafkaListener(
            topics = "${payment-api.kafka.refund-completed-topic}",
            containerFactory = "refundCompletedViewKafkaListenerContainerFactory")
    public void onMessage(RefundCompleted event, Acknowledgment ack) {
        log.info("Consumed refund-completed refundId={}", event.getRefundId());

        useCase.execute(
                new RecordRefundHistoryCommand(
                        UUID.fromString(event.getRefundId()),
                        UUID.fromString(event.getPaymentId()),
                        "COMPLETED",
                        event.getProviderReference(),
                        UUID.fromString(event.getEnvelope().getEventId()),
                        event.getEnvelope().getOccurredAt()));

        ack.acknowledge();
    }
}
