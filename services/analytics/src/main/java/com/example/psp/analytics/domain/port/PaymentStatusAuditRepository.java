package com.example.psp.analytics.domain.port;

import com.example.psp.analytics.domain.model.PaymentStatusAuditEntry;
import java.util.List;

public interface PaymentStatusAuditRepository {

    void saveAll(List<PaymentStatusAuditEntry> entries);
}
