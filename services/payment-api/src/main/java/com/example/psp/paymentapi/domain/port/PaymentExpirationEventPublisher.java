package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Payment;
import java.time.Instant;
import java.util.UUID;

public interface PaymentExpirationEventPublisher {

    void publishExpired(Payment payment, UUID eventId, Instant occurredAt);
}
