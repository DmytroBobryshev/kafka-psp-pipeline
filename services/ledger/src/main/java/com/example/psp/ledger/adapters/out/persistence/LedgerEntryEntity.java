package com.example.psp.ledger.adapters.out.persistence;

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
@Table(name = "ledger_entries")
@Getter
@Setter
@NoArgsConstructor
public class LedgerEntryEntity {

    @Id private UUID id;

    @Column(name = "inbound_event_id", nullable = false)
    private UUID inboundEventId;

    @Column(name = "merchant_id", nullable = false, length = 255)
    private String merchantId;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(nullable = false, length = 10)
    private String direction;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "trace_id", nullable = false, length = 255)
    private String traceId;

    @Column(name = "correlation_id", nullable = false, length = 255)
    private String correlationId;

    @Column(name = "recorded_at", nullable = false)
    private Instant recordedAt;
}
