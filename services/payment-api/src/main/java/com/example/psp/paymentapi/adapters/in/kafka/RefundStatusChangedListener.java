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

/**
 * M23: payment-api's consumer of {@code refunds.refund-status-changed.v1} (PENDING/IPN_RECEIVED/
 * VERIFIED) - group {@code payment-api.refund-status-view.v1}
 * ({@code config.RefundHistoryKafkaConfig}), mirroring
 * {@code adapters.in.kafka.PaymentStatusChangedListener}'s shape for the payment trail. History-
 * only: {@link RecordRefundHistoryUseCase} never touches the {@code Refund} aggregate's own
 * status.
 *
 * <p>No DLQ - same documented scope boundary as {@code PaymentStatusChangedListener} (a derived,
 * lossy read-model listener, ADR-0006).
 */
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

    /** PENDING's providerReference is always {@code ""} on the wire (no provider call yet). */
    private static String blankToNull(String value) {
        return (value == null || value.isBlank()) ? null : value;
    }
}
