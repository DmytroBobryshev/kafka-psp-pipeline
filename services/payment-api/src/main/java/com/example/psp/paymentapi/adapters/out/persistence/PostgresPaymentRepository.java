package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.Payment;
import com.example.psp.paymentapi.domain.model.PaymentPage;
import com.example.psp.paymentapi.domain.model.PaymentStatus;
import com.example.psp.paymentapi.domain.port.PaymentRepository;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
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

    @Override
    public void updateStatus(UUID paymentId, PaymentStatus status) {
        jpaRepository.updateStatus(paymentId, status);
    }

    @Override
    public PaymentPage search(String merchantId, PaymentStatus status, int page, int size) {
        Page<PaymentEntity> result =
                jpaRepository.search(merchantId, status, PageRequest.of(page, size));
        List<Payment> items = result.getContent().stream().map(mapper::toDomain).toList();
        return new PaymentPage(items, page, size, result.getTotalElements());
    }
}
