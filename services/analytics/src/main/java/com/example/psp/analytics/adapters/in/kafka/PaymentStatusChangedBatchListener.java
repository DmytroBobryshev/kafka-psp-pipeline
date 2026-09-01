package com.example.psp.analytics.adapters.in.kafka;

import com.example.psp.analytics.application.RecordPaymentStatusAuditBatchUseCase;
import com.example.psp.analytics.domain.model.PaymentStatusAuditEntry;
import com.example.psp.analytics.domain.port.PartialBatchWriteException;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

@Component
public class PaymentStatusChangedBatchListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentStatusChangedBatchListener.class);

    private final RecordPaymentStatusAuditBatchUseCase useCase;
    private final PaymentStatusChangedAuditMapper mapper;

    public PaymentStatusChangedBatchListener(
            RecordPaymentStatusAuditBatchUseCase useCase, PaymentStatusChangedAuditMapper mapper) {
        this.useCase = useCase;
        this.mapper = mapper;
    }

    @KafkaListener(
            topics = "${analytics.kafka.payment-status-changed-topic}",
            containerFactory = "paymentStatusAuditBatchKafkaListenerContainerFactory")
    public void onBatch(List<PaymentStatusChanged> events) {
        log.debug("Batch listener received {} record(s) in one poll", events.size());

        List<PaymentStatusAuditEntry> entries = events.stream().map(mapper::toEntry).toList();

        try {
            useCase.execute(entries);
        } catch (PartialBatchWriteException ex) {
            throw new BatchListenerFailedException(ex.getMessage(), ex, ex.failedIndex());
        }
    }
}
