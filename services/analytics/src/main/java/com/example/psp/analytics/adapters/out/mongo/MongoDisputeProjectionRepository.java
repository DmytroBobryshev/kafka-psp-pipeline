package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.DisputeProjection;
import com.example.psp.analytics.domain.port.DisputeProjectionRepository;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

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
