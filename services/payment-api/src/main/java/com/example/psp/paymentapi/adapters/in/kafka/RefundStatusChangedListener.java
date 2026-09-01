package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundStatusChanged;
import com.example.psp.paymentapi.application.RecordRefundHistoryCommand;
import com.example.psp.paymentapi.application.RecordRefundHistoryUseCase;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RefundStatusChangedListener {

    private static final Logger log = LoggerFactory.getLogger(RefundStatusChangedListener.class);

    private final RecordRefundHistoryUseCase useCase;

    public RefundStatusChangedListener(RecordRefundHistoryUseCase useCase) {
        this.useCase = useCase;
    }

    @KafkaListener(
            topics = "${payment-api.kafka.refund-status-changed-topic}",
            containerFactory = "refundStatusViewKafkaListenerContainerFactory")
    public void onMessage(RefundStatusChanged event, Acknowledgment ack) {
        log.info(
                "Consumed refund-status-changed refundId={} status={}",
                event.getRefundId(),
                event.getStatus());

        useCase.execute(
                new RecordRefundHistoryCommand(
                        UUID.fromString(event.getRefundId()),
                        UUID.fromString(event.getPaymentId()),
                        event.getStatus(),
                        blankToNull(event.getProviderReference()),
                        UUID.fromString(event.getEnvelope().getEventId()),
                        event.getEnvelope().getOccurredAt()));

        ack.acknowledge();
    }

    private static String blankToNull(String value) {
        return (value == null || value.isBlank()) ? null : value;
    }
}
