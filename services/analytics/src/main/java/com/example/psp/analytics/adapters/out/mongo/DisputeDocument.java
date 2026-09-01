package com.example.psp.analytics.adapters.out.mongo;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "dispute_documents")
@Getter
@Setter
public class DisputeDocument {

    @Id private String id;

    private String paymentId;
    private String merchantId;
    private String reason;
    private long sizeBytes;
    private String sha256;
    private boolean claimChecked;
    private String bucket;
    private String objectKey;
}
