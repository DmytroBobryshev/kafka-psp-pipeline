package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.AuthorizationLatency;
import com.example.psp.analytics.domain.port.AuthorizationLatencyProjectionRepository;
import java.time.Clock;
import java.time.Instant;
import org.springframework.stereotype.Component;

@Component
public class MongoAuthorizationLatencyProjectionRepository
        implements AuthorizationLatencyProjectionRepository {

    private final AuthorizationLatencyMongoRepository mongoRepository;
    private final AuthorizationLatencyMapper mapper;
    private final Clock clock;

    public MongoAuthorizationLatencyProjectionRepository(
            AuthorizationLatencyMongoRepository mongoRepository,
            AuthorizationLatencyMapper mapper,
            Clock clock) {
        this.mongoRepository = mongoRepository;
        this.mapper = mapper;
        this.clock = clock;
    }

    @Override
    public void save(AuthorizationLatency latency) {
        mongoRepository.save(mapper.toDocument(latency, Instant.now(clock)));
    }
}
