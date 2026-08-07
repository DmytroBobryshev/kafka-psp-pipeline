package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Repository;

/**
 * M1 stub persistence adapter: an in-memory map, not a real database (M2/M3 wire PostgreSQL +
 * JPA/outbox, per the stack decisions table in PLAN.md).
 *
 * <p>Deliberately has no JPA {@code @Entity} - there is no persisted schema yet, so there is
 * nothing to apply the "no {@code @Data} on JPA entities" rule to. When the real
 * {@code PaymentEntity} arrives in M3, it belongs here, mapped to/from {@link Payment} by a
 * dedicated MapStruct mapper (one per hexagon boundary), never annotated {@code @Data}.
 */
@Repository
public class InMemoryPaymentRepository implements PaymentRepository {

    private final Map<UUID, Payment> store = new ConcurrentHashMap<>();

    @Override
    public Payment save(Payment payment) {
        store.put(payment.getId(), payment);
        return payment;
    }

    @Override
    public Optional<Payment> findById(UUID id) {
        return Optional.ofNullable(store.get(id));
    }
}
