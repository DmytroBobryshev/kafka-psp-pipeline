package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.ledger.application.ReleaseRefundUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class RefundFailedListener {

    private static final Logger log = LoggerFactory.getLogger(RefundFailedListener.class);

    private final ReleaseRefundUseCase useCase;
    private final RefundFailedMapper mapper;

    public RefundFailedListener(ReleaseRefundUseCase useCase, RefundFailedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${ledger.kafka.refund-failed-topic}",
            containerFactory = "refundFailedKafkaListenerContainerFactory")
    @Transactional("kafkaTransactionManager")
    public void onMessage(RefundFailed event) {
        log.info(
                "Consumed refund-failed eventId={} refundId={} reason={}",
                event.getEnvelope().getEventId(),
                event.getRefundId(),
                event.getReason());

        useCase.execute(mapper.toCommand(event));
    }
}
