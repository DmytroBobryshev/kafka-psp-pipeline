package com.example.psp.webhooknotifier.adapters.out.persistence;

import com.example.psp.webhooknotifier.domain.model.DeliveryAttempt;
import com.example.psp.webhooknotifier.domain.port.DeliveryAttemptLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

/**
 * Real MongoDB adapter for {@link DeliveryAttemptLogRepository}. Talks to the
 * {@code webhook_notifier} database (infra/compose, ADR-0005) via Spring Data MongoDB.
 */
@Repository
public class MongoDeliveryAttemptLogRepository implements DeliveryAttemptLogRepository {

    private static final Logger log = LoggerFactory.getLogger(MongoDeliveryAttemptLogRepository.class);

    private final DeliveryAttemptMongoRepository mongoRepository;
    private final DeliveryAttemptPersistenceMapper mapper;

    public MongoDeliveryAttemptLogRepository(
            DeliveryAttemptMongoRepository mongoRepository, DeliveryAttemptPersistenceMapper mapper) {
        this.mongoRepository = mongoRepository;
        this.mapper = mapper;
    }

    @Override
    public void record(DeliveryAttempt attempt) {
        DeliveryAttemptDocument saved = mongoRepository.save(mapper.toDocument(attempt));
        log.debug(
                "Recorded delivery attempt id={} merchantId={} paymentId={} attempt={} outcome={}",
                saved.getId(),
                attempt.merchantId(),
                attempt.paymentId(),
                attempt.attemptNumber(),
                attempt.outcome());
    }
}
