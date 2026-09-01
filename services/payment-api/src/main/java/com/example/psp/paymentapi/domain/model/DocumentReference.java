package com.example.psp.paymentapi.domain.model;

import java.util.Objects;

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
