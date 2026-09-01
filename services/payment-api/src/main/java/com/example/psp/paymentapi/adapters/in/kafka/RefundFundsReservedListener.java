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

/**
 * M23: payment-api's consumer of {@code refunds.funds-reserved.v1} - group
 * {@code payment-api.refund-funds-reserved-view.v1} ({@code config.RefundHistoryKafkaConfig}), a
 * NEW independent projection alongside psp-connector's own (unrelated) consumer of this same topic
 * (ADR-0005). Records a {@code FUNDS_RESERVED} row in {@code refund_status_history} - this
 * service's own literal, since the event itself carries no status field (no
 * {@code providerReference} either - the ledger mints none) - history-only, same as
 * {@link RefundStatusChangedListener}.
 */
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
