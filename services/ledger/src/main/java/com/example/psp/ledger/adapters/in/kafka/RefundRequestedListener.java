package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundRequested;
import com.example.psp.ledger.application.ReserveRefundUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

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

    }
}
