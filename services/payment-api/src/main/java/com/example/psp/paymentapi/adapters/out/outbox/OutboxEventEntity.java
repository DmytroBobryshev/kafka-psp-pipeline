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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

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
 * <p>{@code payload} is mapped as a plain {@code String} with {@code @JdbcTypeCode(SqlTypes.JSON)}
 * (Hibernate 6's built-in JSON support) rather than a hand-rolled {@code AttributeConverter}: the
 * column is already valid JSON text (produced by {@link OutboxPaymentEventPublisher} via
 * Jackson), so no parsing/transformation is needed at the JPA boundary - only "store this exact
 * string as jsonb, read it back as this exact string."
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

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
