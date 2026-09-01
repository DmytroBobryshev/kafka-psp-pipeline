package com.example.psp.pspconnector.adapters.out.persistence;

import com.example.psp.pspconnector.domain.model.RefundAttempt;
import com.example.psp.pspconnector.domain.port.RefundAttemptLogRepository;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresRefundAttemptLogRepository implements RefundAttemptLogRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresRefundAttemptLogRepository.class);

    private final RefundAttemptJpaRepository jpaRepository;
    private final RefundAttemptPersistenceMapper mapper;

    public PostgresRefundAttemptLogRepository(
            RefundAttemptJpaRepository jpaRepository, RefundAttemptPersistenceMapper mapper) {
        this.jpaRepository = jpaRepository;
        this.mapper = mapper;
    }

    @Override
    public boolean existsByInboundEventId(UUID inboundEventId) {
        return jpaRepository.existsByCausationEventId(inboundEventId);
    }

    @Override
    public Optional<RefundAttempt> findByInboundEventId(UUID inboundEventId) {
        return jpaRepository.findByCausationEventId(inboundEventId).map(mapper::toDomain);
    }

    @Override
    public boolean tryRecord(RefundAttempt attempt) {
        try {
            jpaRepository.saveAndFlush(mapper.toEntity(attempt));
            return true;
        } catch (DataIntegrityViolationException e) {
            log.debug(
                    "Unique constraint rejected duplicate refund attempt refundId={} "
                            + "causationEventId={}",
                    attempt.getRefundId(),
                    attempt.getCausationEventId(),
                    e);
            return false;
        }
    }
}
