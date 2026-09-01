package com.example.psp.pspconnector.adapters.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "payment_attempts")
@Getter
@Setter
@NoArgsConstructor
public class PaymentAttemptEntity {

    @Id
    private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(name = "merchant_id", nullable = false, length = 255)
    private String merchantId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "provider_event_id", nullable = false)
    private UUID providerEventId;

    @Column(nullable = false, length = 20)
    private String outcome;

    @Column(name = "provider_latency_ms", nullable = false)
    private long providerLatencyMs;

    @Column(name = "causation_event_id", nullable = false)
    private UUID causationEventId;

    @Column(name = "inbound_event_id")
    private UUID inboundEventId;

    // Nullable in db/migration/V4 for pre-fix rows - see PaymentAttempt#statusEventId.
    @Column(name = "status_event_id")
    private UUID statusEventId;

    @Column(name = "trace_id", nullable = false, length = 255)
    private String traceId;

    @Column(name = "correlation_id", nullable = false, length = 255)
    private String correlationId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
