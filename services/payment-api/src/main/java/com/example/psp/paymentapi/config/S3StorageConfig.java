package com.example.psp.paymentapi.config;

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
 * S3-compatible client wiring for the M13 claim-check store, against MinIO (root pom's
 * {@code aws-sdk.version} property comment explains the SDK choice).
 *
 * <h2>{@code pathStyleAccessEnabled(true)} is mandatory, not a preference</h2>
 *
 * <p>The AWS SDK defaults to VIRTUAL-HOSTED style addressing ({@code
 * <bucket>.<endpoint>/<key>}), which requires the endpoint to be a wildcard DNS domain that
 * resolves any bucket-name subdomain. MinIO has no such DNS - it is one hostname, and buckets are
 * path segments ({@code <endpoint>/<bucket>/<key>}). Without this flag every S3 call resolves to
 * {@code disputes.minio:9000}, which does not exist, and fails with an opaque connection error
 * that looks like a network problem, not a configuration one.
 *
 * <h2>Region</h2>
 *
 * <p>MinIO ignores the region entirely, but the SDK's request signer (SigV4) still requires a
 * non-null value to compute a signature - a genuinely unused value the protocol nonetheless
 * demands, so a fixed placeholder ({@code us-east-1}) is set rather than left to fail at the
 * first call.
 *
 * <h2>Bucket creation is lazy, not eager at startup</h2>
 *
 * <p>This class does NOT probe MinIO when the Spring context starts - {@link S3Client#builder}
 * makes no network call, only later requests do. Deliberately: {@code
 * PaymentApiApplicationTests}' context-load test (and every other {@code @SpringBootTest} in this
 * service) runs with no MinIO available, following the same "construct lazily, connect on first
 * real call" convention {@code config.KafkaProducerConfig} and {@code config.SchemaRegistryConfig}
 * already establish for the Kafka/Schema-Registry clients - an eager health check here would fail
 * every such test. {@code adapters.out.storage.S3DisputeDocumentStore} creates the bucket
 * on-demand instead, the first time a claim-checked document is actually stored - see its javadoc.
 */
@Configuration
public class S3StorageConfig {

    @Bean
    public S3Client disputeS3Client(
            @Value("${payment-api.disputes.minio.endpoint}") String endpoint,
            @Value("${payment-api.disputes.minio.access-key}") String accessKey,
            @Value("${payment-api.disputes.minio.secret-key}") String secretKey) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }
}
