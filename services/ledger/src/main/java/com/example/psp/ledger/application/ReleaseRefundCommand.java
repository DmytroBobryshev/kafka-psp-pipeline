package com.example.psp.ledger.application;

import java.util.UUID;

public record ReleaseRefundCommand(
        UUID inboundEventId, UUID refundId, String reason, String traceId, String correlationId) {
}
