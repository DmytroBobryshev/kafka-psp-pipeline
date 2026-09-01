package com.example.psp.analytics.adapters.out.s3;

import com.example.psp.analytics.domain.port.DisputeDocumentFetcher;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@Component
public class S3DisputeDocumentFetcher implements DisputeDocumentFetcher {

    private final S3Client s3Client;

    public S3DisputeDocumentFetcher(S3Client disputeS3Client) {
        this.s3Client = disputeS3Client;
    }

    @Override
    public byte[] fetch(String bucket, String objectKey) {
        ResponseBytes<GetObjectResponse> response =
                s3Client.getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(objectKey).build());
        return response.asByteArray();
    }
}
