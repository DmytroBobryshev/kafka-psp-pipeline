package com.example.psp.pspconnector.adapters.out.persistence;

import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.port.AttemptLogRepository;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;

/**
 * Real Postgres adapter for {@link AttemptLogRepository}. Talks to the {@code psp_connector}
 * database (infra/compose, ADR-0005) via Spring Data JPA ({@link PaymentAttemptJpaRepository});
 * {@link PaymentAttemptPersistenceMapper} keeps the JPA entity out of {@code domain/} and
 * {@code application/} entirely (ADR-0007).
 *
 * <p>M5: {@link #tryRecord} is the DB-constraint-is-the-authority half of the idempotent
 * consumer. It is not wrapped in {@code @Transactional} - {@code JpaRepository.saveAndFlush}
 * already runs as its own self-contained transaction (Spring Data's default propagation), so the
 * unique-constraint violation surfaces synchronously from this call, inside this method's own
 * try/catch, rather than being deferred to some later, uncontrolled flush point. If a future
 * milestone wraps the calling use case in a broader {@code @Transactional} (M6/M7), this call
 * would need {@code Propagation.REQUIRES_NEW} to keep a caught constraint violation here from
 * poisoning the outer transaction - noted for whoever adds that annotation, not solved here.
 */
@Repository
public class PostgresAttemptLogRepository implements AttemptLogRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresAttemptLogRepository.class);

    private final PaymentAttemptJpaRepository jpaRepository;
    private final PaymentAttemptPersistenceMapper mapper;

    public PostgresAttemptLogRepository(
            PaymentAttemptJpaRepository jpaRepository, PaymentAttemptPersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public boolean existsByInboundEventId(UUID inboundEventId) {
        return jpaRepository.existsByInboundEventId(inboundEventId);
    }

    @Override
    public boolean existsByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId) {
        return jpaRepository.existsByPaymentIdAndProviderEventId(paymentId, providerEventId);
    }

    @Override
    public boolean tryRecord(PaymentAttempt attempt) {
        try {
            jpaRepository.saveAndFlush(mapper.toEntity(attempt));
            return true;
        } catch (DataIntegrityViolationException e) {
            // Lost a check-then-act race in ProcessPaymentRequestUseCase: some other thread or
            // consumer instance inserted a row colliding on EITHER unique constraint - the level 1
            // uq_payment_attempts_inbound_event_id (V2) or the level 2
            // uq_payment_attempts_payment_provider_event (V1) - between one of the two
            // existsBy... pre-checks and this insert. That is a normal outcome of at-least-once
            // delivery under concurrency, not an error - reported by return value, never rethrown
            // (AttemptLogRepository#tryRecord's contract). Which constraint actually fired is not
            // inspected here; ProcessPaymentRequestUseCase re-derives it with one cheap follow-up
            // read purely to keep its dedup-reason counters honest on this rare path.
            log.debug(
                    "Unique constraint rejected duplicate attempt paymentId={} providerEventId={} "
                            + "inboundEventId={}",
                    attempt.getPaymentId(),
                    attempt.getProviderEventId(),
                    attempt.getCausationEventId(),
                    e);
            return false;
        }
    }

    @Override
    public Optional<PaymentAttempt> findLatestByPaymentId(UUID paymentId) {
        return jpaRepository.findFirstByPaymentIdOrderByProcessedAtDesc(paymentId).map(mapper::toDomain);
    }

    @Override
    public Optional<PaymentAttempt> findByInboundEventId(UUID inboundEventId) {
        return jpaRepository.findByInboundEventId(inboundEventId).map(mapper::toDomain);
    }

    @Override
    public Optional<PaymentAttempt> findByPaymentIdAndProviderEventId(UUID paymentId, UUID providerEventId) {
        return jpaRepository.findByPaymentIdAndProviderEventId(paymentId, providerEventId).map(mapper::toDomain);
    }
}
