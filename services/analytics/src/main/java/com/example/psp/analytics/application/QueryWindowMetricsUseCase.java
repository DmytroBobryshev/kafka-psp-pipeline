package com.example.psp.analytics.application;

import com.example.psp.analytics.domain.model.MerchantConfigSnapshot;
import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import com.example.psp.analytics.domain.port.MetricsProjectionRepository;
import com.example.psp.analytics.domain.port.WindowMetricsQueryPort;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;

@Service
public class QueryWindowMetricsUseCase {

    private final WindowMetricsQueryPort queryPort;
    private final MetricsProjectionRepository projectionRepository;

    public QueryWindowMetricsUseCase(
            WindowMetricsQueryPort queryPort, MetricsProjectionRepository projectionRepository) {
        this.queryPort = queryPort;
        this.projectionRepository = projectionRepository;
    }

    public boolean stateStoreReady() {
        return queryPort.storeReady();
    }

    public String streamsClientState() {
        return queryPort.clientState();
    }

    public List<MerchantMetricsWindow> liveWindowsFor(String merchantId, Duration lookback) {
        Instant now = Instant.now();
        return queryPort.windowsFor(merchantId, now.minus(lookback), now);
    }

    public List<MerchantMetricsWindow> liveWindows(Duration lookback) {
        Instant now = Instant.now();
        return queryPort.allWindows(now.minus(lookback), now);
    }

    public List<MerchantMetricsWindow> projectedWindowsFor(String merchantId, Duration lookback) {
        return projectionRepository.findByMerchantSince(merchantId, Instant.now().minus(lookback));
    }

    public Optional<MerchantConfigSnapshot> merchantConfig(String merchantId) {
        return queryPort.merchantConfig(merchantId);
    }
}
