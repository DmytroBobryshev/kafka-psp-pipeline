package com.example.psp.ledger.application;

import com.example.psp.ledger.domain.model.Money;
import java.util.UUID;

public record RecordLedgerEntryCommand(
        UUID inboundEventId,
        UUID paymentId,
        String merchantId,
        Money amount,
        String status,
        String traceId,
        String correlationId) {
}
