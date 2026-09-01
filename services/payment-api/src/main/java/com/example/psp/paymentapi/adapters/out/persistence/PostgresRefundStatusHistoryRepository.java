package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.RefundStatusHistoryEntry;
import com.example.psp.paymentapi.domain.port.RefundStatusHistoryRepository;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresRefundStatusHistoryRepository implements RefundStatusHistoryRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresRefundStatusHistoryRepository.class);

    private final RefundStatusHistoryJpaRepository jpaRepository;
    private final RefundStatusHistoryPersistenceMapper mapper;

    public PostgresRefundStatusHistoryRepository(
            RefundStatusHistoryJpaRepository jpaRepository, RefundStatusHistoryPersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public boolean tryRecord(RefundStatusHistoryEntry entry) {
        try {
            jpaRepository.saveAndFlush(mapper.toEntity(entry));
            return true;
        } catch (DataIntegrityViolationException e) {
            log.debug(
                    "Unique constraint rejected duplicate refund status history row refundId={} eventId={} status={}",
                    entry.getRefundId(),
                    entry.getEventId(),
                    entry.getStatus(),
                    e);
            return false;
        }
    }

    @Override
    public List<RefundStatusHistoryEntry> findByRefundId(UUID refundId) {
        return jpaRepository.findByRefundIdOrderByOccurredAtAsc(refundId).stream()
                .map(mapper::toDomain)
                .toList();
    }
}
