package com.example.psp.paymentapi.adapters.out.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * JPA entity for the {@code outbox_event} table (M6; schema owned by
 * {@code db/migration/V2__create_outbox_event_table.sql}, Flyway-managed - ADR-0005). Saved by
 * {@link OutboxEventJpaRepository} inside the SAME transaction as {@code PaymentEntity} - both go
 * through the same {@code EntityManager}/{@code DataSource}, so wrapping the caller
 * ({@code application.CreatePaymentUseCase}) in {@code @Transactional} is what makes the payment
 * row and the outbox row commit atomically. This class never talks to Kafka; a separate process
 * (Debezium, via Kafka Connect) is the only thing that ever reads this table, and it reads the
 * write-ahead log, not this JPA mapping.
 *
 * <p>Same Lombok convention as {@code adapters.out.persistence.PaymentEntity}: {@code @Getter}/
 * {@code @Setter}/{@code @NoArgsConstructor}, never {@code @Data} on a JPA entity (PLAN.md rule -
 * identity should be id-based, not value-based, and a generated {@code toString()} risks
 * triggering lazy-loading on associations).
 *
 * <p>{@code payload} is a plain {@code byte[]}, mapped by Hibernate's PostgreSQL dialect straight
 * to {@code bytea} with no annotation needed (M9 Phase 1 - was {@code String} +
 * {@code @JdbcTypeCode(SqlTypes.JSON)} through M6). The bytes are the COMPLETE Confluent Avro
 * wire format - magic byte + 4-byte schema id + Avro binary - produced by
 * {@link OutboxPaymentEventPublisher} via {@code KafkaAvroSerializer}, not JSON text: see
 * {@code db/migration/V3__outbox_event_payload_bytes.sql} and the README's M9 section for the
 * outbox-serialization decision this column embodies.
 */
@Entity
@Table(name = "outbox_event")
@Getter
@Setter
@NoArgsConstructor
public class OutboxEventEntity {

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false, length = 255)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, length = 255)
    private String aggregateId;

    @Column(name = "event_type", nullable = false, length = 255)
    private String eventType;

    @Column(nullable = false)
    private byte[] payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    /**
     * M15: the W3C {@code traceparent} of the span active when this row was staged, captured via
     * Micrometer's {@code Tracer}/{@code Propagator} (never hand-formatted - see
     * {@code OutboxPaymentEventPublisher}). Nullable: no active span means no trace context to
     * bridge, and this column is simply left null (see {@code db/migration/V5__outbox_event_trace_parent.sql}
     * for what that degrades to downstream). Debezium's outbox event router places it back onto
     * the relayed Kafka record as the {@code traceparent} header
     * ({@code infra/compose/connect/payment-outbox-connector.json}) - this is the bridge across
     * the outbox hop the M6 architecture would otherwise break.
     */
    @Column(name = "trace_parent", length = 64)
    private String traceParent;
}
