package com.example.psp.analytics.config;

import java.net.URI;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * S3-compatible client wiring for the M13 claim-check dereference ({@code adapters.out.s3.
 * S3DisputeDocumentFetcher}), against MinIO. Mirrors payment-api's {@code config.
 * S3StorageConfig} exactly - same path-style-access requirement, same placeholder region - see
 * that class's javadoc for the full reasoning. This side only ever calls GetObject, never
 * PutObject/CreateBucket, so unlike payment-api's it has nothing analogous to "ensure the bucket
 * exists" to do.
 *
 * <p>Constructing an {@link S3Client} makes no network call - {@code AnalyticsApplicationTests}'
 * context-load test runs with no MinIO available, the same "construct lazily, connect on first
 * real call" convention every Kafka/Schema-Registry client in this service already follows.
 */
@Configuration
public class S3StorageConfig {

    @Bean
    public S3Client disputeS3Client(
            @Value("${analytics.disputes.minio.endpoint}") String endpoint,
            @Value("${analytics.disputes.minio.access-key}") String accessKey,
            @Value("${analytics.disputes.minio.secret-key}") String secretKey) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }
}
