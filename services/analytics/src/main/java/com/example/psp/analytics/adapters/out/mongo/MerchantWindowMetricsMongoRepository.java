package com.example.psp.analytics.adapters.out.mongo;

import java.time.Instant;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface MerchantWindowMetricsMongoRepository
        extends MongoRepository<MerchantWindowMetricsDocument, String> {

    List<MerchantWindowMetricsDocument> findByMerchantIdAndWindowStartGreaterThanEqualOrderByWindowStartDesc(
            String merchantId, Instant from);
}
