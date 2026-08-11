package com.example.psp.analytics.adapters.out.mongo;

import java.time.Instant;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * MongoDB projection of one (merchant, 1-minute window) result (M10). Collection
 * {@code merchant_metrics_1m} in the {@code analytics} database (ADR-0005 / PLAN.md's persistence
 * table).
 *
 * <p><b>The {@code _id} is the idempotency key.</b> It is
 * {@code merchantId|windowStartEpochMillis} (see
 * {@code domain.model.MerchantMetricsWindow#key()}), so a {@code save()} of a re-emitted window
 * <i>replaces</i> its own document. That is what makes the projection safe under
 * {@code processing.guarantee=at_least_once} and under the deliberate absence of
 * {@code suppress()}: the topology emits intermediate results for an open window many times, and
 * every one of them overwrites the last rather than appending. Any other id choice - a generated
 * ObjectId, an insert-only write - would turn a normal Streams emit pattern into duplicate rows.
 *
 * <p><b>Derived values are stored, not just the counters.</b> The state store holds counters
 * ({@code totalCount}/{@code declinedCount}/{@code latencySumMillis}) because an aggregate must
 * be composable; the projection holds {@code declineRate} and {@code avgPipelineLatencyMillis} as
 * well because its readers are dashboards and ad-hoc {@code mongosh} queries, which should not
 * have to re-derive them. The counters are kept alongside so the derivation is auditable.
 *
 * <p>Plain getters/setters, not {@code @Data} (PLAN.md's persistence-entity rule): Spring Data
 * needs a no-arg constructor and setters, and a generated {@code equals}/{@code hashCode} over
 * every field on a persistence entity is the classic source of subtle identity bugs.
 */
@Document(collection = "merchant_metrics_1m")
@CompoundIndex(name = "merchant_window_idx", def = "{'merchantId': 1, 'windowStart': -1}")
@Getter
@Setter
public class MerchantWindowMetricsDocument {

    /** {@code merchantId|windowStartEpochMillis} - see the class javadoc. */
    @Id private String id;

    private String merchantId;

    /** From the joined {@code GlobalKTable}; null when no config has ever joined. */
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

    /**
     * Wall-clock time of the last write. Distinct from {@code windowEnd}: a window that closed at
     * 12:01:00 can still be rewritten at 12:01:25 by a record arriving inside the grace period,
     * and the gap between these two fields is the most direct evidence a grace period is doing
     * anything.
     */
    private Instant updatedAt;
}
