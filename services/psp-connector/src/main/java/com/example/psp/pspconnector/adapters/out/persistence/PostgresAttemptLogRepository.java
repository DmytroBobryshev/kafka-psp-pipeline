package com.example.psp.pspconnector.adapters.out.persistence;

import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.port.AttemptLogRepository;
import org.springframework.stereotype.Repository;

/**
 * Real Postgres adapter for {@link AttemptLogRepository}. Talks to the {@code psp_connector}
 * database (infra/compose, ADR-0005) via Spring Data JPA ({@link PaymentAttemptJpaRepository});
 * {@link PaymentAttemptPersistenceMapper} keeps the JPA entity out of {@code domain/} and
 * {@code application/} entirely (ADR-0007).
 */
@Repository
public class PostgresAttemptLogRepository implements AttemptLogRepository {

    private final PaymentAttemptJpaRepository jpaRepository;
    private final PaymentAttemptPersistenceMapper mapper;

    public PostgresAttemptLogRepository(
            PaymentAttemptJpaRepository jpaRepository, PaymentAttemptPersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public void record(PaymentAttempt attempt) {
        jpaRepository.save(mapper.toEntity(attempt));
    }
}
