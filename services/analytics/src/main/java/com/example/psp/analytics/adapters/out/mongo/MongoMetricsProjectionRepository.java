package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import com.example.psp.analytics.domain.port.MetricsProjectionRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.stereotype.Component;

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
