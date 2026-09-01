package com.example.psp.pspconnector.adapters.out.persistence;

import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.port.AttemptLogRepository;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;

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
