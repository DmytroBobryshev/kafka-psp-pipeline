package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.PaymentStatusHistoryEntry;
import com.example.psp.paymentapi.domain.port.PaymentStatusHistoryRepository;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;

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
