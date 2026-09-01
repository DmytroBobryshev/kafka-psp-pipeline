package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import java.util.List;
import java.util.UUID;

public interface PaymentStatusHistoryRepository {

    boolean tryRecord(PaymentStatusHistoryEntry entry);

    List<PaymentStatusHistoryEntry> findByPaymentId(UUID paymentId);
}
