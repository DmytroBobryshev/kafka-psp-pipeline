package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import com.example.psp.analytics.domain.model.MerchantWindowMetrics;
import java.time.Instant;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MetricsProjectionMapper {

    @Mapping(target = "id", expression = "java(window.key())")
    @Mapping(target = "merchantId", source = "window.merchantId")
    @Mapping(target = "merchantDisplayName", source = "window.metrics.merchantDisplayName")
    @Mapping(target = "windowStart", source = "window.windowStart")
    @Mapping(target = "windowEnd", source = "window.windowEnd")
    @Mapping(target = "totalCount", source = "window.metrics.totalCount")
    @Mapping(target = "declinedCount", source = "window.metrics.declinedCount")
    @Mapping(target = "latencySumMillis", source = "window.metrics.latencySumMillis")
    @Mapping(target = "declineRate", expression = "java(window.metrics().declineRate())")
    @Mapping(target = "declineRateBps", expression = "java(window.metrics().declineRateBps())")
    @Mapping(
            target = "avgPipelineLatencyMillis",
            expression = "java(window.metrics().avgPipelineLatencyMillis())")
    @Mapping(
            target = "declineRateAlertThresholdBps",
            source = "window.metrics.declineRateAlertThresholdBps")
    @Mapping(target = "declineRateAlert", expression = "java(window.metrics().declineRateAlert())")
    @Mapping(target = "updatedAt", source = "updatedAt")
    MerchantWindowMetricsDocument toDocument(MerchantMetricsWindow window, Instant updatedAt);

    @Mapping(target = "merchantId", source = "merchantId")
    @Mapping(target = "windowStart", source = "windowStart")
    @Mapping(target = "windowEnd", source = "windowEnd")
    @Mapping(target = "metrics", expression = "java(toMetrics(document))")
    MerchantMetricsWindow toDomain(MerchantWindowMetricsDocument document);

    default MerchantWindowMetrics toMetrics(MerchantWindowMetricsDocument document) {
        return new MerchantWindowMetrics(
                document.getTotalCount(),
                document.getDeclinedCount(),
                document.getLatencySumMillis(),
                document.getMerchantDisplayName(),
                document.getDeclineRateAlertThresholdBps());
    }
}
