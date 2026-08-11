package com.example.psp.analytics.adapters.out.mongo;

import java.time.Instant;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * MongoDB projection of one payment's authorization latency (M13). Collection
 * {@code authorization_latency} in the {@code analytics} database (ADR-0005).
 *
 * <p>{@code _id = paymentId}: a payment is decided exactly once, so unlike M10's composite key
 * this is a plain natural key. {@code save()} on a document whose {@code _id} is already set is
 * an upsert (replace-by-id, same mechanism as
 * {@code adapters.out.mongo.MongoMetricsProjectionRepository}), which is what makes a re-emitted
 * join result (at-least-once redelivery, a rebalance replaying the join window) converge on the
 * same document rather than duplicate it.
 *
 * <p>Plain getters/setters, not {@code @Data} (PLAN.md's persistence-entity rule) - same
 * reasoning as {@code MerchantWindowMetricsDocument}.
 */
@Document(collection = "authorization_latency")
@Getter
@Setter
public class AuthorizationLatencyDocument {

    /** {@code paymentId} - see the class javadoc. */
    @Id private String id;

    private String merchantId;
    private String providerReference;
    private String status;
    private boolean declined;

    private Instant requestedAt;
    private Instant decidedAt;
    private long latencyMillis;

    /** Wall-clock time of the write - the M13 analogue of M10's document `updatedAt`. */
    private Instant projectedAt;
}
