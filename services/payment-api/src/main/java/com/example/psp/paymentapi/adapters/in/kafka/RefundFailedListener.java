package com.example.psp.paymentapi.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.paymentapi.application.RecordRefundHistoryCommand;
import com.example.psp.paymentapi.application.RecordRefundHistoryUseCase;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * M23: payment-api's consumer of {@code refunds.refund-failed.v1} - group
 * {@code payment-api.refund-failed-view.v1} ({@code config.RefundHistoryKafkaConfig}). Records a
 * terminal {@code FAILED} row in {@code refund_status_history} (no {@code providerReference} - the
 * event carries none, see {@code 10-refund-failed.avsc}) - history-only, same as
 * {@link RefundStatusChangedListener}.
 */
@Component
public class RefundFailedListener {

    private static final Logger log = LoggerFactory.getLogger(RefundFailedListener.class);

    private final RecordRefundHistoryUseCase useCase;

    public RefundFailedListener(RecordRefundHistoryUseCase useCase) {
        this.useCase = useCase;
    }

    @KafkaListener(
            topics = "${payment-api.kafka.refund-failed-topic}",
            containerFactory = "refundFailedViewKafkaListenerContainerFactory")
    public void onMessage(RefundFailed event, Acknowledgment ack) {
        log.info("Consumed refund-failed refundId={} reason={}", event.getRefundId(), event.getReason());

        useCase.execute(
                new RecordRefundHistoryCommand(
                        UUID.fromString(event.getRefundId()),
                        UUID.fromString(event.getPaymentId()),
                        "FAILED",
                        null,
                        UUID.fromString(event.getEnvelope().getEventId()),
                        event.getEnvelope().getOccurredAt()));

        ack.acknowledge();
    }
}
