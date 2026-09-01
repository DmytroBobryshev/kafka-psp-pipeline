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
            log.info("MinIO bucket '{}' was created by a concurrent instance", bucket);
        }
    }
}
