package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Payment;
import java.util.Optional;
import java.util.UUID;

/**
 * Outbound port for persisting payments. Implemented by {@code adapters/out/persistence} - the
 * domain depends only on this interface, never on JPA or any concrete storage technology
 * (ADR-0007).
 */
public interface PaymentRepository {

    Payment save(Payment payment);

    Optional<Payment> findById(UUID id);
}
