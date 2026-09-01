package com.example.psp.analytics.application;

import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import com.example.psp.analytics.domain.model.MerchantWindowMetrics;
import com.example.psp.analytics.domain.port.MetricsProjectionRepository;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
