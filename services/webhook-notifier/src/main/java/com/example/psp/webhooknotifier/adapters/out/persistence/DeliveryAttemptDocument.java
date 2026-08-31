package com.example.psp.webhooknotifier.adapters.out.persistence;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * MongoDB document for one row of the M8 delivery-attempt log, {@code webhook_notifier.delivery_attempts}
 * (ADR-0005: this service owns this database exclusively). No {@code @Data} - same rule PLAN.md
 * gives JPA entities (equals/hashCode pitfalls), applied here too even though this is a Mongo
 * document rather than a JPA entity, for the same reason: {@code @Getter}/{@code @Setter}/
 * {@code @Builder} only.
 *
 * <h2>The TTL index</h2>
 *
 * <p>{@code attemptedAt} backs a TTL index created programmatically (not via a static
 * {@code @Indexed(expireAfterSeconds=...)}, which requires a compile-time constant) so its
 * duration stays a runtime property, {@code webhook-notifier.mongo.attempt-log-ttl-seconds}
 * (default 2,592,000s / 30 days) - see {@code config.MongoIndexConfig}, which creates it once at
 * startup. MongoDB's TTL monitor deletes a document once {@code attemptedAt + ttl} is in the
 * past, checked in a background sweep (roughly every 60s), not instantaneously on expiry.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "delivery_attempts")
public class DeliveryAttemptDocument {

    @Id private String id;
    private String merchantId;
    private String paymentId;
    // M19: nullable - null for a payment status-change delivery, set for a refund one.
    private String refundId;
    // M19: "PAYMENT_STATUS_CHANGED" / "REFUND_COMPLETED" / "REFUND_FAILED" - see
    // domain.model.WebhookDeliveryCommand#eventType().
    private String eventType;
    // M19: the grouping key adapters.out.persistence.MongoDeliveryAttemptLogRepository#search
    // aggregates on - identical across every retry-tier attempt of one logical delivery.
    private String causationEventId;
    private int attemptNumber;
    private String outcome;
    private Integer statusCode;
    private String error;
    private String sourceTopic;
    private Instant attemptedAt;
}
