package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.common.events.DomainEvent;
import com.example.psp.common.events.EventEnvelope;
import java.math.BigDecimal;
import java.util.UUID;

public record PaymentRequestedEvent(
        EventEnvelope envelope,
        UUID paymentId,
        String merchantId,
        BigDecimal amount,
        String currency,
        String status)
        implements DomainEvent {
}
