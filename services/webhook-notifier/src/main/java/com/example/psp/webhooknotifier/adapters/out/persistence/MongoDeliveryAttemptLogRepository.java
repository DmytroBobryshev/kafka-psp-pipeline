package com.example.psp.webhooknotifier.adapters.out.persistence;

import com.example.psp.webhooknotifier.domain.model.DeliveryAttempt;
import com.example.psp.webhooknotifier.domain.model.WebhookDelivery;
import com.example.psp.webhooknotifier.domain.port.DeliveryAttemptLogRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Repository;

@Repository
public class MongoDeliveryAttemptLogRepository implements DeliveryAttemptLogRepository {

    private static final Logger log = LoggerFactory.getLogger(MongoDeliveryAttemptLogRepository.class);
    private static final String COLLECTION = "delivery_attempts";

    private final DeliveryAttemptMongoRepository mongoRepository;
    private final DeliveryAttemptPersistenceMapper mapper;
    private final MongoTemplate mongoTemplate;

    public MongoDeliveryAttemptLogRepository(
            DeliveryAttemptMongoRepository mongoRepository,
            DeliveryAttemptPersistenceMapper mapper,
            MongoTemplate mongoTemplate) {
        this.mongoRepository = mongoRepository;
        this.mapper = mapper;
        this.mongoTemplate = mongoTemplate;
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

    @Override
    public List<WebhookDelivery> search(UUID paymentId, UUID refundId, String merchantId, int limit) {
        List<Criteria> filters = new ArrayList<>();
        if (paymentId != null) {
            filters.add(Criteria.where("paymentId").is(paymentId.toString()));
        }
        if (refundId != null) {
            filters.add(Criteria.where("refundId").is(refundId.toString()));
        }
        if (merchantId != null) {
            filters.add(Criteria.where("merchantId").is(merchantId));
        }

        List<AggregationOperation> stages = new ArrayList<>();
        if (!filters.isEmpty()) {
            stages.add(Aggregation.match(new Criteria().andOperator(filters.toArray(new Criteria[0]))));
        }
        stages.add(Aggregation.sort(Sort.Direction.DESC, "attemptedAt"));
        stages.add(
                Aggregation.group("causationEventId")
                        .first("causationEventId")
                        .as("id")
                        .first("eventType")
                        .as("eventType")
                        .first("paymentId")
                        .as("paymentId")
                        .first("refundId")
                        .as("refundId")
                        .first("merchantId")
                        .as("merchantId")
                        .first("outcome")
                        .as("status")
                        .count()
                        .as("attempts")
                        .first("attemptedAt")
                        .as("lastAttemptAt")
                        .min("attemptedAt")
                        .as("createdAt"));
        // "Sort newest first" for the deliveries list itself, not just within one group.
        stages.add(Aggregation.sort(Sort.Direction.DESC, "lastAttemptAt"));
        stages.add(Aggregation.limit(limit));

        Aggregation aggregation = Aggregation.newAggregation(stages);
        AggregationResults<DeliveryGroup> results =
                mongoTemplate.aggregate(aggregation, COLLECTION, DeliveryGroup.class);

        return results.getMappedResults().stream().map(MongoDeliveryAttemptLogRepository::toDomain).toList();
    }

    private static WebhookDelivery toDomain(DeliveryGroup group) {
        return new WebhookDelivery(
                UUID.fromString(group.id()),
                group.eventType(),
                UUID.fromString(group.paymentId()),
                group.refundId() == null ? null : UUID.fromString(group.refundId()),
                group.merchantId(),
                group.status(),
                group.attempts(),
                group.lastAttemptAt(),
                group.createdAt());
    }

    private record DeliveryGroup(
            String id,
            String eventType,
            String paymentId,
            String refundId,
            String merchantId,
            String status,
            int attempts,
            Instant lastAttemptAt,
            Instant createdAt) {}
}
