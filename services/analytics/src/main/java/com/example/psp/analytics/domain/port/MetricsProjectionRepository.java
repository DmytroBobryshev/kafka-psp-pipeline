package com.example.psp.analytics.domain.port;

import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import java.time.Instant;
import java.util.List;

public interface MetricsProjectionRepository {

    void save(MerchantMetricsWindow window);

    List<MerchantMetricsWindow> findByMerchantSince(String merchantId, Instant from);
}
