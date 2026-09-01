package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Refund;
import java.time.Instant;
import java.util.UUID;

public interface RefundExpirationEventPublisher {

    void publishExpired(Refund refund, UUID eventId, Instant occurredAt);
}
