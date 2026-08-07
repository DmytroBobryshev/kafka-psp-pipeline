package com.example.psp.common.events;

/**
 * Marker interface for concrete per-topic event types.
 *
 * <p>Per ADR-0002, each topic's event is one flat record carrying an {@link EventEnvelope} plus
 * its own domain fields at the top level - there is no generic {@code payload} field:
 *
 * <pre>{@code
 * public record PaymentRequested(
 *         EventEnvelope envelope,
 *         String paymentId,
 *         String merchantId,
 *         BigDecimal amount,
 *         String currency) implements DomainEvent {
 * }
 * }</pre>
 *
 * <p>A Java record whose first component is named {@code envelope} automatically satisfies this
 * interface's accessor - no boilerplate required beyond declaring {@code implements DomainEvent}.
 */
public interface DomainEvent {

    EventEnvelope envelope();
}
