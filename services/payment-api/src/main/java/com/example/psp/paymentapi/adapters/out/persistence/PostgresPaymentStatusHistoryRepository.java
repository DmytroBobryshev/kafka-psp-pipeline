package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import com.example.psp.paymentapi.domain.port.PaymentStatusHistoryRepository;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;

/**
 * Real Postgres adapter for {@link PaymentStatusHistoryRepository} (M20). Talks to the
 * {@code payment_api} database (infra/compose, ADR-0005) via Spring Data JPA
 * ({@link PaymentStatusHistoryJpaRepository}); {@link PaymentStatusHistoryPersistenceMapper}
 * keeps the JPA entity out of {@code domain/} and {@code application/} entirely (ADR-0007).
 *
 * <p>{@link #tryRecord} is the DB-constraint-is-the-authority half of the idempotent insert - the
 * exact same convention (and, deliberately, the exact same code shape) as psp-connector's
 * {@code PostgresAttemptLogRepository#tryRecord}: {@code saveAndFlush} rather than {@code save},
 * so the unique-constraint violation (V9's {@code uq_payment_status_history_event_id}) surfaces
 * synchronously from this call, inside this method's own try/catch, instead of being deferred to
 * whatever later flush point the surrounding transaction happens to hit. Not wrapped in its own
 * {@code @Transactional} for the same reason as that class: {@code saveAndFlush} already runs as
 * its own self-contained unit under Spring Data's default propagation.
 */
@Repository
public class PostgresPaymentStatusHistoryRepository implements PaymentStatusHistoryRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresPaymentStatusHistoryRepository.class);

    private final PaymentStatusHistoryJpaRepository jpaRepository;
    private final PaymentStatusHistoryPersistenceMapper mapper;

    public PostgresPaymentStatusHistoryRepository(
            PaymentStatusHistoryJpaRepository jpaRepository, PaymentStatusHistoryPersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public boolean tryRecord(PaymentStatusHistoryEntry entry) {
        try {
            jpaRepository.saveAndFlush(mapper.toEntity(entry));
            return true;
        } catch (DataIntegrityViolationException e) {
            // Lost the race, or more likely just a plain redelivery: some earlier delivery of the
            // SAME payments.payment-status-changed.v1 event already inserted this eventId. Normal
            // at-least-once behaviour, not an error - reported by return value, never rethrown
            // (PaymentStatusHistoryRepository#tryRecord's contract).
            log.debug(
                    "Unique constraint rejected duplicate status history row paymentId={} eventId={} status={}",
                    entry.getPaymentId(),
                    entry.getEventId(),
                    entry.getStatus(),
                    e);
            return false;
        }
    }

    @Override
    public List<PaymentStatusHistoryEntry> findByPaymentId(UUID paymentId) {
        return jpaRepository.findByPaymentIdOrderByOccurredAtAsc(paymentId).stream()
                .map(mapper::toDomain)
                .toList();
    }
}
