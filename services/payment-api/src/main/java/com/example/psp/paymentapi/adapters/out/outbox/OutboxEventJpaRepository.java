package com.example.psp.paymentapi.adapters.out.outbox;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository - the actual SQL-generating layer behind
 * {@link OutboxPaymentEventPublisher}, same convention as
 * {@code adapters.out.persistence.PaymentJpaRepository}. Not a hexagon port itself: the domain
 * never sees this type, only {@link com.example.psp.paymentapi.domain.port.PaymentEventPublisher}
 * (ADR-0007).
 */
public interface OutboxEventJpaRepository extends JpaRepository<OutboxEventEntity, UUID> {
}
