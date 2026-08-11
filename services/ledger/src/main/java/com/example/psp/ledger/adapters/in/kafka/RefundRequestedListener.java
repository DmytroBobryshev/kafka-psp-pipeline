package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundRequested;
import com.example.psp.ledger.application.ReserveRefundUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * M11 step 2: listens on {@code refunds.refund-requested.v1} using the SAME transactional
 * machinery M7's {@link com.example.psp.ledger.adapters.in.kafka.PaymentStatusChangedListener}
 * established - {@code containerFactory} wires
 * {@code ContainerProperties.setKafkaAwareTransactionManager(...)} (see
 * {@code config.RefundKafkaConsumerConfig}), so this method runs inside the same per-record Kafka
 * transaction shape: no {@code Acknowledgment} parameter, offsets committed atomically with
 * whatever this use case publishes, via the container - not by anything in this class. See
 * {@code PaymentStatusChangedListener}'s javadoc for the full mechanics; unchanged here.
 */
@Component
public class RefundRequestedListener {

    private static final Logger log = LoggerFactory.getLogger(RefundRequestedListener.class);

    private final ReserveRefundUseCase useCase;
    private final RefundRequestedMapper mapper;

    public RefundRequestedListener(ReserveRefundUseCase useCase, RefundRequestedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${ledger.kafka.refund-requested-topic}",
            containerFactory = "refundRequestedKafkaListenerContainerFactory")
    @Transactional("kafkaTransactionManager")
    public void onMessage(RefundRequested event) {
        log.info(
                "Consumed refund-requested eventId={} refundId={} paymentId={} merchantId={} amount={}",
                event.getEnvelope().getEventId(),
                event.getRefundId(),
                event.getPaymentId(),
                event.getMerchantId(),
                event.getAmount());

        useCase.execute(mapper.toCommand(event));

        // No ack.acknowledge() - returning normally is what commits both the produced event
        // (refunds.funds-reserved.v1 or refunds.refund-failed.v1) and the consumed offset,
        // together, exactly like PaymentStatusChangedListener.
    }
}
