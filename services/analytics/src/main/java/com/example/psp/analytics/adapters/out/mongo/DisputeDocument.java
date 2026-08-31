package com.example.psp.analytics.adapters.out.mongo;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * MongoDB projection of one dispute (M13). Collection {@code dispute_documents} in the
 * {@code analytics} database (ADR-0005).
 *
 * <p>{@code _id = disputeId} - same idempotency shape every other projection in this service uses
 * ({@code PaymentStatusAuditDocument}'s javadoc): a redelivered event upserts the same document
 * rather than duplicating it.
 */
@Document(collection = "dispute_documents")
@Getter
@Setter
public class DisputeDocument {

    /** {@code disputeId} - see the class javadoc. */
    @Id private String id;

    private String paymentId;
    private String merchantId;
    private String reason;
    private long sizeBytes;
    private String sha256;
    private boolean claimChecked;
    private String bucket;
    private String objectKey;
}
