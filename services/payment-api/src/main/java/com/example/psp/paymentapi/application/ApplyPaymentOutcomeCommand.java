package com.example.psp.paymentapi.application;

import com.example.psp.paymentapi.domain.model.PaymentStatus;
import java.util.UUID;

/**
 * Application-layer input model for {@link ApplyPaymentOutcomeUseCase} (M19). Deliberately
 * separate from the inbound Kafka event ({@code com.example.psp.common.events.avro.PaymentStatusChanged})
 * - the use case never depends on an Avro wire type, only on this plain command, the same
 * "adapter maps to a command, use case never sees the wire shape" rule every other use case in
 * this module already follows (compare {@link CreatePaymentCommand}, driven by the web adapter
 * instead of a Kafka one).
 */
public record ApplyPaymentOutcomeCommand(UUID paymentId, PaymentStatus status) {
}
