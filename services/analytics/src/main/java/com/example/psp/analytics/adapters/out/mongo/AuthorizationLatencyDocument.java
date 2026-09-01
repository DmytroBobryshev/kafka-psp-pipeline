package com.example.psp.analytics.adapters.out.mongo;

import java.time.Instant;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "authorization_latency")
@Getter
@Setter
public class AuthorizationLatencyDocument {

    @Id private String id;

    private String merchantId;
    private String providerReference;
    private String status;
    private boolean declined;

    private Instant requestedAt;
    private Instant decidedAt;
    private long latencyMillis;

    private Instant projectedAt;
}
