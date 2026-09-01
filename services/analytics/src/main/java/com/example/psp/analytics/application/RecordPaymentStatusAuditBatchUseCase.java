package com.example.psp.analytics.application;

import com.example.psp.analytics.domain.model.PaymentStatusAuditEntry;
import com.example.psp.analytics.domain.port.PaymentStatusAuditRepository;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
