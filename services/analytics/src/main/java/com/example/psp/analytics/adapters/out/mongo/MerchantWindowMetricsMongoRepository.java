package com.example.psp.analytics.adapters.out.mongo;

import java.time.Instant;
import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Spring Data repository for the projection collection (M10). A framework interface, so it lives
 * in {@code adapters/out} and is never referenced from {@code application/} or {@code domain/} -
 * {@code domain.port.MetricsProjectionRepository} is what those layers see (ADR-0007).
 */
public interface MerchantWindowMetricsMongoRepository
        extends MongoRepository<MerchantWindowMetricsDocument, String> {

    /** Backed by the {@code merchant_window_idx} compound index on the document. */
    List<MerchantWindowMetricsDocument> findByMerchantIdAndWindowStartGreaterThanEqualOrderByWindowStartDesc(
            String merchantId, Instant from);
}
