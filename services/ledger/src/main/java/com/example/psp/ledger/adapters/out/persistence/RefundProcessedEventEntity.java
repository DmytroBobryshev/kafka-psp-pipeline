package com.example.psp.ledger.adapters.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "refund_processed_events")
@Getter
@Setter
@NoArgsConstructor
public class RefundProcessedEventEntity {

    @Id
    @Column(name = "inbound_event_id")
    private UUID inboundEventId;

    @Column(name = "refund_id", nullable = false)
    private UUID refundId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
