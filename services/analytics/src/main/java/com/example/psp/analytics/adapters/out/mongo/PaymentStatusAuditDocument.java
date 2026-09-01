package com.example.psp.analytics.adapters.out.mongo;

import java.time.Instant;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "payment_status_audit")
@Getter
@Setter
public class PaymentStatusAuditDocument {

    @Id private String id;

    private String paymentId;
    private String merchantId;
    private String status;
    private Instant occurredAt;
}
