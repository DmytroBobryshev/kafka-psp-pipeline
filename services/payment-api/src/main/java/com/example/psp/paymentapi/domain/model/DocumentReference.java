package com.example.psp.paymentapi.domain.model;

import java.util.Objects;

/**
 * The result of a claim-check upload (M13) - what {@link com.example.psp.paymentapi.domain.port.DisputeDocumentStore}
 * hands back after putting a dispute document in MinIO, and what
 * {@link com.example.psp.paymentapi.domain.port.DisputeEventPublisher} turns into the Avro
 * {@code ClaimCheckReference} union branch. Field-for-field identical to that Avro record on
 * purpose (see {@code adapters.out.kafka.DisputeAvroEventFactory}) - this is the domain-side,
 * Avro-free copy ADR-0007 requires.
 */
public record DocumentReference(String bucket, String objectKey, long sizeBytes, String contentType) {

    public DocumentReference {
        Objects.requireNonNull(bucket, "bucket must not be null");
        Objects.requireNonNull(objectKey, "objectKey must not be null");
        Objects.requireNonNull(contentType, "contentType must not be null");
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must be >= 0, was " + sizeBytes);
        }
    }
}
