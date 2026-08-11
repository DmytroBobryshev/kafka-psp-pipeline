package com.example.psp.paymentapi.domain.port;

import com.example.psp.paymentapi.domain.model.Refund;

/**
 * Outbound port for publishing {@code refunds.refund-requested.v1} (M11) - the refund-path
 * counterpart of {@link PaymentEventPublisher}. Same M6 outbox story: implemented by
 * {@code adapters.out.outbox.OutboxRefundEventPublisher}, which does no I/O to Kafka at all - it
 * inserts a row into the SAME {@code outbox_event} table {@code PaymentEventPublisher} uses, via
 * plain JPA, so that "save the refund row" and "stage the event" commit atomically in one Postgres
 * transaction (ADR-0004: commands enter via REST; everything else is events - and the outbox is
 * what makes publishing that event as durable as the write that triggered it).
 */
public interface RefundEventPublisher {

    void publishRefundRequested(Refund refund);
}
