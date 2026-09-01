package com.example.psp.ledger.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.ledger.application.RecordLedgerEntryUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class PaymentStatusChangedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusChangedListener.class);

    private final RecordLedgerEntryUseCase useCase;
    private final PaymentStatusChangedMapper mapper;

    public PaymentStatusChangedListener(
            RecordLedgerEntryUseCase useCase, PaymentStatusChangedMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${ledger.kafka.payment-status-changed-topic}",
            containerFactory = "paymentStatusChangedKafkaListenerContainerFactory")
    @Transactional("kafkaTransactionManager")
    public void onMessage(PaymentStatusChanged event) {
        log.info(
                "Consumed payment-status-changed eventId={} paymentId={} merchantId={} status={}",
                event.getEnvelope().getEventId(),
                event.getPaymentId(),
                event.getMerchantId(),
                event.getStatus());

        useCase.execute(mapper.toCommand(event));

    }
}
