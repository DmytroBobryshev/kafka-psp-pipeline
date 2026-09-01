package com.example.psp.analytics.adapters.in.web;

import com.example.psp.analytics.domain.model.MerchantConfigSnapshot;
import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import java.time.Instant;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface AnalyticsWebMapper {

    @Mapping(target = "merchantId", source = "window.merchantId")
    @Mapping(target = "merchantDisplayName", source = "window.metrics.merchantDisplayName")
    @Mapping(target = "windowStart", source = "window.windowStart")
    @Mapping(target = "windowEnd", source = "window.windowEnd")
    @Mapping(target = "open", expression = "java(now.isBefore(window.windowEnd()))")
    @Mapping(target = "totalCount", source = "window.metrics.totalCount")
    @Mapping(target = "declinedCount", source = "window.metrics.declinedCount")
    @Mapping(target = "declineRate", expression = "java(window.metrics().declineRate())")
    @Mapping(target = "declineRateBps", expression = "java(window.metrics().declineRateBps())")
    @Mapping(
            target = "avgPipelineLatencyMillis",
            expression = "java(window.metrics().avgPipelineLatencyMillis())")
    @Mapping(
            target = "declineRateAlertThresholdBps",
            source = "window.metrics.declineRateAlertThresholdBps")
    @Mapping(target = "declineRateAlert", expression = "java(window.metrics().declineRateAlert())")
    WindowMetricsResponse toResponse(MerchantMetricsWindow window, Instant now);

    MerchantConfigResponse toResponse(MerchantConfigSnapshot snapshot);
}
