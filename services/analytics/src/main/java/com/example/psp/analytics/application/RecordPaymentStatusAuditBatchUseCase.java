package com.example.psp.analytics.application;

import com.example.psp.analytics.domain.model.PaymentStatusAuditEntry;
import com.example.psp.analytics.domain.port.PaymentStatusAuditRepository;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The M13 batch listener's use case: one bulk write per batch. {@code application/} depends on
 * the port only (ADR-0007) and stays free of both {@code org.springframework.kafka} (so it
 * cannot construct or catch {@code BatchListenerFailedException} itself - that translation has to
 * happen in {@code adapters.in.kafka.PaymentStatusChangedBatchListener}, the only place allowed
 * to know Spring Kafka exists) and Mongo types.
 */
@Service
public class RecordPaymentStatusAuditBatchUseCase {

    private static final Logger log =
            LoggerFactory.getLogger(RecordPaymentStatusAuditBatchUseCase.class);

    private final PaymentStatusAuditRepository repository;

    public RecordPaymentStatusAuditBatchUseCase(PaymentStatusAuditRepository repository) {
        this.repository = repository;
    }

    public void execute(List<PaymentStatusAuditEntry> entries) {
        repository.saveAll(entries);
        log.info("Bulk-wrote {} payment-status-audit entries in one write", entries.size());
    }
}
