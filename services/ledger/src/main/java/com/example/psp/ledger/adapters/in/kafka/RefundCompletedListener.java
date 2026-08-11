package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.ledger.application.SettleRefundUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * M11 step 4 (happy path): listens on {@code refunds.refund-completed.v1}, same transactional
 * shape as {@link RefundRequestedListener} - see that class's javadoc and
 * {@code PaymentStatusChangedListener}'s (M7) for the full mechanics.
 */
@Component
public class RefundCompletedListener {

    private static final Logger log = LoggerFactory.getLogger(RefundCompletedListener.class);

    private final SettleRefundUseCase useCase;
    private final RefundCompletedMapper mapper;

    public RefundCompletedListener(SettleRefundUseCase useCase, RefundCompletedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${ledger.kafka.refund-completed-topic}",
            containerFactory = "refundCompletedKafkaListenerContainerFactory")
    @Transactional("kafkaTransactionManager")
    public void onMessage(RefundCompleted event) {
        log.info(
                "Consumed refund-completed eventId={} refundId={} paymentId={} merchantId={} "
                        + "providerReference={}",
                event.getEnvelope().getEventId(),
                event.getRefundId(),
                event.getPaymentId(),
                event.getMerchantId(),
                event.getProviderReference());

        useCase.execute(mapper.toCommand(event));
    }
}
