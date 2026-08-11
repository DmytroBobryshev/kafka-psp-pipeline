package com.example.psp.pspconnector.application;

import com.example.psp.pspconnector.domain.model.Money;
import java.util.UUID;

/**
 * Application-layer input model for {@link ExecuteRefundUseCase}. Deliberately separate from the
 * inbound wire event ({@code com.example.psp.common.events.avro.FundsReserved}) - the Kafka
 * adapter maps its event onto this command, same pattern as {@code ProcessPaymentRequestCommand}.
 *
 * @param causationEventId the inbound {@code refunds.funds-reserved.v1} event's own
 *                         {@code eventId} - M5 level 1's idempotency key, checked BEFORE calling
 *                         the provider (see {@link ExecuteRefundUseCase}).
 * @param traceId          propagated from the inbound envelope.
 * @param correlationId    propagated from the inbound envelope.
 */
public record ExecuteRefundCommand(
        UUID refundId,
        UUID paymentId,
        String merchantId,
        Money amount,
        UUID causationEventId,
        String traceId,
        String correlationId) {
}
