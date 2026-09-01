package com.example.psp.analytics.domain.port;

import com.example.psp.analytics.domain.model.MerchantConfigSnapshot;
import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface WindowMetricsQueryPort {

    boolean storeReady();

    String clientState();

    List<MerchantMetricsWindow> windowsFor(String merchantId, Instant from, Instant to);

    List<MerchantMetricsWindow> allWindows(Instant from, Instant to);

    Optional<MerchantConfigSnapshot> merchantConfig(String merchantId);
}
