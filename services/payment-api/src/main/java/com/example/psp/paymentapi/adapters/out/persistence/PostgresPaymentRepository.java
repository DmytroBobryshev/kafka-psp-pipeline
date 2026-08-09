package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Repository;

/**
 * Real Postgres adapter for {@link PaymentRepository} (M3), replacing the M1 in-memory stub
 * ({@code InMemoryPaymentRepository}, deleted). Talks to the {@code payment_api} database
 * (infra/compose, ADR-0005) via Spring Data JPA ({@link PaymentJpaRepository});
 * {@link PaymentPersistenceMapper} keeps the JPA entity out of {@code domain/} and
 * {@code application/} entirely (ADR-0007) - callers of this class see only {@link Payment}.
 */
@Repository
public class PostgresPaymentRepository implements PaymentRepository {

    private final PaymentJpaRepository jpaRepository;
    private final PaymentPersistenceMapper mapper;

    public PostgresPaymentRepository(PaymentJpaRepository jpaRepository, PaymentPersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public Payment save(Payment payment) {
        PaymentEntity saved = jpaRepository.save(mapper.toEntity(payment));
        return mapper.toDomain(saved);
    }

    @Override
    public Optional<Payment> findById(UUID id) {
        return jpaRepository.findById(id).map(mapper::toDomain);
    }
}
