package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.time.Instant;
import java.util.UUID;

public record ApplyPaymentOutcomeCommand(
        UUID paymentId,
        PaymentStatus domainStatus,
        String rawStatus,
        String providerReference,
        UUID eventId,
        Instant occurredAt) {
}
