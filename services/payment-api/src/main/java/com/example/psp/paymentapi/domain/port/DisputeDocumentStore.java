package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.DocumentReference;

/**
 * Outbound port for the claim-check upload (M13) - implemented by {@code adapters.out.storage.
 * S3DisputeDocumentStore} against MinIO's S3-compatible API. Only called when
 * {@link com.example.psp.paymentapi.domain.model.ClaimCheckPolicy#requiresClaimCheck} says yes;
 * a small document never reaches this port at all (see
 * {@code application.OpenDisputeUseCase}) - the whole point of the pattern is that the common
 * case (a small dispute document) costs zero extra network hops.
 */
public interface DisputeDocumentStore {

    /**
     * Uploads {@code documentBytes} and returns the reference the event will carry.
     *
     * @param disputeId   becomes the MinIO object key (see the Avro schema's {@code
     *                    ClaimCheckReference.objectKey} doc for why: it makes the reference
     *                    reconstructible from the event's own {@code disputeId} with no lookup).
     * @param documentBytes the raw bytes - already known to exceed the claim-check threshold.
     * @param contentType the caller-supplied MIME type, stored as the object's content type and
     *                    mirrored into the reference so a consumer never has to fetch the object
     *                    just to know how to render it.
     */
    DocumentReference store(String disputeId, byte[] documentBytes, String contentType);
}
