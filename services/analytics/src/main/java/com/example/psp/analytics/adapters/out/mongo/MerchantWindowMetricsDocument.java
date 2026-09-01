package com.example.psp.analytics.adapters.out.mongo;

import java.time.Instant;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "merchant_metrics_1m")
@CompoundIndex(name = "merchant_window_idx", def = "{'merchantId': 1, 'windowStart': -1}")
@Getter
@Setter
public class MerchantWindowMetricsDocument {

    @Id private String id;

    private String merchantId;

    private String merchantDisplayName;

    private Instant windowStart;
    private Instant windowEnd;

    private long totalCount;
    private long declinedCount;
    private long latencySumMillis;

    private double declineRate;
    private long declineRateBps;
    private double avgPipelineLatencyMillis;

    private Integer declineRateAlertThresholdBps;
    private boolean declineRateAlert;

    private Instant updatedAt;
}
