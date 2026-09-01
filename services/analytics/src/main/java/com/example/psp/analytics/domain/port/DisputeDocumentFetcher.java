package com.example.psp.analytics.domain.port;

public interface DisputeDocumentFetcher {

    byte[] fetch(String bucket, String objectKey);
}
