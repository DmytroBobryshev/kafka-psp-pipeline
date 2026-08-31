package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.DocumentReference;
import java.util.UUID;

/**
 * Outbound port for {@code disputes.dispute-opened.v1} (M13). Two methods, deliberately, the same
 * shape {@link MerchantConfigPublisher} already established for "the record's shape depends on a
 * branch decided before this port is called": {@link #publishInline} and
 * {@link #publishClaimChecked} are never both called for the same dispute, and keeping them as
 * two narrow methods (rather than one method taking a document that is sometimes bytes and
 * sometimes a reference) means a caller cannot accidentally construct the nonsensical third case
 * - "here are both the bytes AND a reference" - the type system that {@code adapters.out.kafka.
 * DisputeAvroEventFactory}'s Avro union also forbids at the wire level.
 *
 * <h2>Why this does NOT go through the M6 outbox</h2>
 *
 * <p>Same reasoning as {@link MerchantConfigPublisher}'s javadoc, restated for this port: opening
 * a dispute is a single write. payment-api keeps no {@code dispute} table (the Kafka topic, and
 * downstream analytics' Mongo projection, are the system of record for a dispute's existence -
 * this module ships no dispute query API), so there is no second local write for an outbox row to
 * be atomic with. Publishing directly and blocking on the result - like {@code
 * adapters.out.kafka.KafkaMerchantConfigPublisher} - means a broker outage surfaces as a failed
 * {@code POST} rather than a request that returns 202 for a dispute that never left the building.
 */
public interface DisputeEventPublisher {

    /**
     * Publishes with the document's bytes inline in the event value - the common case
     * ({@link com.example.psp.paymentapi.domain.model.ClaimCheckPolicy} said the document is at
     * or below the threshold).
     */
    void publishInline(
            UUID disputeId,
            UUID paymentId,
            String merchantId,
            String reason,
            byte[] documentBytes,
            String contentType);

    /**
     * Publishes with a {@code ClaimCheckReference} instead of the bytes - {@code reference} was
     * already durably uploaded to MinIO by {@link DisputeDocumentStore#store} before this method
     * is called, so the event never claims a document exists anywhere it does not yet.
     */
    void publishClaimChecked(
            UUID disputeId, UUID paymentId, String merchantId, String reason, DocumentReference reference);
}
