package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.AuthorizationLatency;
import com.example.psp.analytics.domain.port.AuthorizationLatencyProjectionRepository;
import java.time.Clock;
import java.time.Instant;
import org.springframework.stereotype.Component;

/**
 * MongoDB adapter for {@link AuthorizationLatencyProjectionRepository} (M13). Same shape as
 * {@code MongoMetricsProjectionRepository} (M10): {@code MongoRepository#save} on a document
 * whose {@code _id} is already set is an upsert, and the write happens synchronously on a Kafka
 * Streams stream thread, inside the M13 join's terminal {@code foreach} - so an unreachable
 * MongoDB becomes consumer lag on this application's tasks, and an exception here is
 * deliberately left uncaught (it kills the stream thread, which is the visible, correct failure
 * mode - see M10 README's "Compromises" for the same trade-off, unchanged here).
 */
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
