package com.example.psp.paymentapi.adapters.out.storage;

import com.example.psp.paymentapi.domain.model.DocumentReference;
import com.example.psp.paymentapi.domain.port.DisputeDocumentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * S3-compatible adapter for {@link DisputeDocumentStore} (M13), against MinIO
 * ({@code config.S3StorageConfig} builds the client). Only ever called for a document that
 * {@link com.example.psp.paymentapi.domain.model.ClaimCheckPolicy} already decided exceeds the
 * threshold - see {@code application.OpenDisputeUseCase}.
 *
 * <p>Object key = {@code disputeId} (the port's own javadoc explains why: it makes the reference
 * reconstructible from the event alone). One bucket for the whole system
 * ({@code payment-api.disputes.minio.bucket}, default {@code disputes}) - there is exactly one
 * producer of claim-checked documents in this system, so a per-topic or per-service bucket
 * naming scheme would be ceremony with no consumer.
 *
 * <h2>Bucket creation happens HERE, lazily, on the first upload - not at startup</h2>
 *
 * <p>MinIO does not auto-create buckets, and {@code config.S3StorageConfig}'s javadoc explains
 * why nothing probes MinIO when the Spring context starts (every {@code @SpringBootTest} in this
 * service would fail with no MinIO available). {@link #store} instead tries the upload first and,
 * on {@link NoSuchBucketException}, creates the bucket once and retries - the same "converge to
 * the desired state on demand" shape as a lazy Kafka producer connecting on its first {@code
 * send()}. In steady state (bucket already exists) this costs nothing extra: the happy path is a
 * single {@code PutObject} call, and the create-and-retry branch only ever runs once per bucket's
 * lifetime, on whichever pod happens to handle the first claim-checked dispute after a fresh
 * MinIO volume.
 */
@Component
public class S3DisputeDocumentStore implements DisputeDocumentStore {

    private static final Logger log = LoggerFactory.getLogger(S3DisputeDocumentStore.class);

    private final S3Client s3Client;
    private final String bucket;

    public S3DisputeDocumentStore(
            S3Client disputeS3Client, @Value("${payment-api.disputes.minio.bucket}") String bucket) {
        this.s3Client = disputeS3Client;
        this.bucket = bucket;
    }

    @Override
    public DocumentReference store(String disputeId, byte[] documentBytes, String contentType) {
        PutObjectRequest request =
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(disputeId)
                        .contentType(contentType)
                        .contentLength((long) documentBytes.length)
                        .build();
        RequestBody body = RequestBody.fromBytes(documentBytes);

        try {
            s3Client.putObject(request, body);
        } catch (NoSuchBucketException missingBucket) {
            createBucketIfAbsent();
            s3Client.putObject(request, body);
        }

        return new DocumentReference(bucket, disputeId, documentBytes.length, contentType);
    }

    private void createBucketIfAbsent() {
        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
            log.info("Created MinIO bucket '{}' on first claim-checked upload", bucket);
        } catch (BucketAlreadyOwnedByYouException racedCreate) {
            // Another instance won the create race between our failed putObject and this
            // createBucket call - the end state (bucket exists) is identical, so this is not an
            // error, just a log line worth having.
            log.info("MinIO bucket '{}' was created by a concurrent instance", bucket);
        }
    }
}
