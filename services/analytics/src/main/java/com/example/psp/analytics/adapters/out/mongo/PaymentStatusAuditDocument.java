package com.example.psp.analytics.adapters.out.mongo;

import java.time.Instant;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * MongoDB projection of one raw payment-status-change event (M13's batch listener). Collection
 * {@code payment_status_audit} in the {@code analytics} database (ADR-0005).
 *
 * <p>{@code _id = eventId} (envelope.eventId, ADR-0002's idempotency key) - a redelivered batch
 * (rebalance, restart before the offset commit) upserts the same documents rather than
 * duplicating them, the same idempotency shape every other projection in this service uses.
 */
@Document(collection = "payment_status_audit")
@Getter
@Setter
public class PaymentStatusAuditDocument {

    /** {@code envelope.eventId} - see the class javadoc. */
    @Id private String id;

    private String paymentId;
    private String merchantId;
    private String status;
    private Instant occurredAt;
}
