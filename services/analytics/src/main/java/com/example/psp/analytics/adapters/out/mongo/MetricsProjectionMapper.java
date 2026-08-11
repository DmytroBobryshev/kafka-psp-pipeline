package com.example.psp.analytics.adapters.out.mongo;

import com.example.psp.analytics.domain.model.MerchantMetricsWindow;
import com.example.psp.analytics.domain.model.MerchantWindowMetrics;
import java.time.Instant;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the persistence boundary (ADR-0007, "MapStruct at every boundary"): domain
 * {@code <->} MongoDB document.
 *
 * <p>Most of the interest is in the <b>flattening</b>: the domain nests
 * {@link MerchantWindowMetrics} inside {@link MerchantMetricsWindow}, while the document is flat
 * so a {@code mongosh} query or a dashboard aggregation does not have to reach through a
 * sub-object. The derived fields ({@code declineRate}, {@code declineRateBps},
 * {@code avgPipelineLatencyMillis}, {@code declineRateAlert}) come from the domain's own accessor
 * methods, so the definition of "decline rate" exists in exactly one place - the domain record -
 * and cannot drift between the REST response and the stored document.
 *
 * <p>{@code unmappedTargetPolicy = ERROR}: a field added to the document without a mapping fails
 * the build. {@code updatedAt} is supplied by the caller rather than mapped, hence the explicit
 * source parameter.
 */
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

    /**
     * Rebuilds the aggregate from the stored counters - never from the stored derived values,
     * which are a denormalised convenience for readers, not the source of truth.
     */
    default MerchantWindowMetrics toMetrics(MerchantWindowMetricsDocument document) {
        return new MerchantWindowMetrics(
                document.getTotalCount(),
                document.getDeclinedCount(),
                document.getLatencySumMillis(),
                document.getMerchantDisplayName(),
                document.getDeclineRateAlertThresholdBps());
    }
}
