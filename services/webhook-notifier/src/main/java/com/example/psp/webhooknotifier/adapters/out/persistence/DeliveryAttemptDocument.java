package com.example.psp.webhooknotifier.adapters.out.persistence;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "delivery_attempts")
public class DeliveryAttemptDocument {

    @Id private String id;
    private String merchantId;
    private String paymentId;
    // M19: nullable - null for a payment status-change delivery, set for a refund one.
    private String refundId;
    private String eventType;
    private String causationEventId;
    private int attemptNumber;
    private String outcome;
    private Integer statusCode;
    private String error;
    private String sourceTopic;
    private Instant attemptedAt;
}
