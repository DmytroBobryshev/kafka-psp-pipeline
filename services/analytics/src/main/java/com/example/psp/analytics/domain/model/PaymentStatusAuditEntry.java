package com.example.psp.analytics.domain.model;

import java.time.Instant;

public record PaymentStatusAuditEntry(
        String eventId, String paymentId, String merchantId, String status, Instant occurredAt) {}
