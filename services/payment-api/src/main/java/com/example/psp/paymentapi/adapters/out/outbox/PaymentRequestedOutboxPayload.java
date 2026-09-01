package com.example.psp.paymentapi.adapters.out.outbox;

import com.example.psp.common.events.DomainEvent;
import com.example.psp.common.events.EventEnvelope;
import java.math.BigDecimal;
import java.util.UUID;

public record PaymentRequestedOutboxPayload(
        EventEnvelope envelope,
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status)
        implements DomainEvent {
}
