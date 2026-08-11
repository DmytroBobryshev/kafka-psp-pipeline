package com.example.psp.analytics.application;

import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import com.example.psp.analytics.domain.model.MerchantWindowMetrics;
import com.example.psp.analytics.domain.port.MetricsProjectionRepository;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Persists one windowed aggregate result to the MongoDB projection (M10).
 *
 * <p>Called from the topology's terminal {@code foreach}, once per emitted update of a (merchant,
 * window) pair. How <i>often</i> that is depends on two Streams settings, not on this class:
 * {@code statestore.cache.max.bytes} and {@code commit.interval.ms}. The record cache collapses
 * repeated updates to the same key and flushes on commit, so a merchant taking 500 payments a
 * minute produces on the order of one Mongo write per commit interval, not 500. Turning the cache
 * off (a common "why don't I see every update?" reflex) turns this into a write per record - see
 * the README's "Every configurable knob".
 *
 * <p>{@code application/} depends on the port, never on Mongo (ADR-0007), and is deliberately
 * free of Kafka types too: it receives the window bounds as plain {@link Instant}s, so the same
 * use case would serve a REST backfill or a test harness unchanged.
 */
@Service
public class ProjectWindowMetricsUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProjectWindowMetricsUseCase.class);

    private final MetricsProjectionRepository projectionRepository;

    public ProjectWindowMetricsUseCase(MetricsProjectionRepository projectionRepository) {
        this.projectionRepository = projectionRepository;
    }

    public void project(String merchantId, Instant windowStart, Instant windowEnd, MerchantWindowMetrics metrics) {
        MerchantMetricsWindow window =
                new MerchantMetricsWindow(merchantId, windowStart, windowEnd, metrics);

        projectionRepository.save(window);

        if (log.isDebugEnabled()) {
            log.debug(
                    "Projected window merchantId={} window=[{}, {}) count={} declineRate={} avgLatencyMs={} alert={}",
                    merchantId,
                    windowStart,
                    windowEnd,
                    metrics.totalCount(),
                    metrics.declineRate(),
                    metrics.avgPipelineLatencyMillis(),
                    metrics.declineRateAlert());
        }
    }
}
