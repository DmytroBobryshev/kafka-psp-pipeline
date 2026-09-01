package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.RefundStatusHistoryEntry;
import java.util.List;
import java.util.UUID;

public interface RefundStatusHistoryRepository {

    boolean tryRecord(RefundStatusHistoryEntry entry);

    List<RefundStatusHistoryEntry> findByRefundId(UUID refundId);
}
