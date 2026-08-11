package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import com.example.psp.analytics.domain.port.MetricsProjectionRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * MongoDB adapter for {@link MetricsProjectionRepository} (M10).
 *
 * <p>{@code MongoRepository#save} on a document whose {@code _id} is already set is an upsert
 * (replace-by-id), which is the whole idempotency story - see
 * {@link MerchantWindowMetricsDocument}. No {@code @Transactional}: a single-document write in
 * MongoDB is atomic on its own, and there is nothing else here to make atomic with it. Wrapping
 * it in a transaction would also require a replica set, which the compose stack's single-node
 * mongod is not.
 *
 * <p>This write happens on a <b>Kafka Streams stream thread</b>, inside the topology's terminal
 * {@code foreach}. Two consequences that are easy to get wrong:
 *
 * <ul>
 *   <li>It is synchronous and it blocks the stream thread. A slow or unreachable MongoDB directly
 *       becomes consumer lag - which is the correct, visible failure mode, and better than a
 *       fire-and-forget queue that silently drops projections.</li>
 *   <li>An exception thrown here propagates into Streams and kills the stream thread (the
 *       default {@code StreamsUncaughtExceptionHandler} replaces it). It is deliberately not
 *       caught: silently swallowing a projection failure would leave the store and the projection
 *       permanently disagreeing, with nothing to notice it. Deferred hardening is recorded in the
 *       README's "Compromises".</li>
 * </ul>
 */
@Component
public class MongoMetricsProjectionRepository implements MetricsProjectionRepository {

    private final MerchantWindowMetricsMongoRepository mongoRepository;
    private final MetricsProjectionMapper mapper;
    private final Clock clock;

    public MongoMetricsProjectionRepository(
            MerchantWindowMetricsMongoRepository mongoRepository,
            MetricsProjectionMapper mapper,
            Clock clock) {
        this.mongoRepository = mongoRepository;
        this.mapper = mapper;
        this.clock = clock;
    }

    @Override
    public void save(MerchantMetricsWindow window) {
        mongoRepository.save(mapper.toDocument(window, Instant.now(clock)));
    }

    @Override
    public List<MerchantMetricsWindow> findByMerchantSince(String merchantId, Instant from) {
        return mongoRepository
                .findByMerchantIdAndWindowStartGreaterThanEqualOrderByWindowStartDesc(merchantId, from)
                .stream()
                .map(mapper::toDomain)
                .toList();
    }
}
