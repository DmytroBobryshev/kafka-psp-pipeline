package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.DisputeProjection;
import com.example.psp.analytics.domain.port.DisputeProjectionRepository;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

/**
 * MongoDB adapter for {@link DisputeProjectionRepository} (M13). A single {@code save()} per
 * event - unlike {@code MongoPaymentStatusAuditRepository}'s bulk-write shape, there is no batch
 * to collapse here (see the port's javadoc): {@code MongoTemplate#save} upserts by {@code _id}
 * ({@code DisputeDocument#getId}, mapped from {@code disputeId}) on its own, which is exactly the
 * idempotent-redelivery behaviour this projection needs.
 */
@Component
public class MongoDisputeProjectionRepository implements DisputeProjectionRepository {

    private final MongoTemplate mongoTemplate;
    private final DisputeProjectionMapper mapper;

    public MongoDisputeProjectionRepository(MongoTemplate mongoTemplate, DisputeProjectionMapper mapper) {
        this.mongoTemplate = mongoTemplate;
        this.mapper = mapper;
    }

    @Override
    public void save(DisputeProjection projection) {
        mongoTemplate.save(mapper.toDocument(projection));
    }
}
