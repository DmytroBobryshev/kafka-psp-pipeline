package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.Refund;
import com.example.psp.paymentapi.domain.port.RefundRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Repository;

/**
 * Real Postgres adapter for {@link RefundRepository} (M11). Talks to the {@code payment_api}
 * database (infra/compose, ADR-0005) via Spring Data JPA.
 */
@Repository
public class PostgresRefundRepository implements RefundRepository {

    private final RefundJpaRepository jpaRepository;
    private final RefundPersistenceMapper mapper;

    public PostgresRefundRepository(RefundJpaRepository jpaRepository, RefundPersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public Refund save(Refund refund) {
        RefundEntity saved = jpaRepository.save(mapper.toEntity(refund));
        return mapper.toDomain(saved);
    }

    @Override
    public BigDecimal sumRequestedAmount(UUID paymentId) {
        return jpaRepository.sumAmountByPaymentId(paymentId);
    }

    @Override
    public List<Refund> findByPaymentId(UUID paymentId) {
        return jpaRepository.findByPaymentId(paymentId).stream().map(mapper::toDomain).toList();
    }

    @Override
    public Optional<Refund> findByIdAndPaymentId(UUID id, UUID paymentId) {
        return jpaRepository.findByIdAndPaymentId(id, paymentId).map(mapper::toDomain);
    }

    @Override
    public List<Refund> findExpirationCandidates(Instant now) {
        return jpaRepository.findExpirationCandidates(now).stream().map(mapper::toDomain).toList();
    }
}
