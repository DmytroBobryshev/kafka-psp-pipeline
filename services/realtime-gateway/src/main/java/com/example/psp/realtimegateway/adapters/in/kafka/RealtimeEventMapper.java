package com.example.psp.realtimegateway.adapters.in.kafka;

import com.example.psp.common.events.avro.EventEnvelope;
import com.example.psp.common.events.avro.FundsReserved;
import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.common.events.avro.RefundRequested;
import com.example.psp.common.events.avro.ReservationReleased;
import com.example.psp.realtimegateway.domain.model.RealtimeEvent;
import org.springframework.stereotype.Component;

/**
 * Converts whichever of the 7 generated Avro classes this gateway consumes into the
 * transport-agnostic {@link RealtimeEvent} (ADR-0007: {@code domain/} must not know Avro exists).
 *
 * <p>A plain hand-written class, not a MapStruct {@code @Mapper} - the same established exception
 * every {@code *AvroEventFactory} in this codebase uses (see
 * {@code payment-api}'s {@code PaymentAvroEventFactory} javadoc for the reasoning), applied here
 * in the opposite direction (Avro -&gt; domain, fanning IN from 7 source shapes instead of
 * fanning out to one). Java 21 pattern-matching {@code switch} (JEP 441, final since Java 21) is
 * what makes a single method a clean, exhaustive dispatch instead of 7 near-duplicate listener
 * methods - see {@code adapters.in.kafka.RealtimeEventListener}, which is why this gateway needs
 * only ONE {@code @KafkaListener} for all 7 topics: every generated Avro class shares no common
 * interface beyond {@code SpecificRecordBase}, but the deserializer (with
 * {@code specific.avro.reader=true}) already hands back the correct concrete type per record, so
 * {@code instanceof}-pattern dispatch on the runtime type is exactly the right tool.
 */
@Component
public class RealtimeEventMapper {

    public RealtimeEvent toDomain(Object avroEvent) {
        return switch (avroEvent) {
            case PaymentRequested e ->
                    build(e.getEnvelope(), e.getPaymentId(), e.getMerchantId(), null, e.getStatus(), null, null);
            case PaymentStatusChanged e ->
                    build(
                            e.getEnvelope(),
                            e.getPaymentId(),
                            e.getMerchantId(),
                            null,
                            e.getStatus(),
                            e.getDeclineReason(),
                            e.getProviderReference());
            case RefundRequested e ->
                    build(
                            e.getEnvelope(),
                            e.getPaymentId(),
                            e.getMerchantId(),
                            e.getRefundId(),
                            "REQUESTED",
                            e.getReason(),
                            null);
            case FundsReserved e ->
                    build(
                            e.getEnvelope(),
                            e.getPaymentId(),
                            e.getMerchantId(),
                            e.getRefundId(),
                            "RESERVED",
                            null,
                            null);
            case RefundCompleted e ->
                    build(
                            e.getEnvelope(),
                            e.getPaymentId(),
                            e.getMerchantId(),
                            e.getRefundId(),
                            "COMPLETED",
                            null,
                            e.getProviderReference());
            case RefundFailed e ->
                    build(
                            e.getEnvelope(),
                            e.getPaymentId(),
                            e.getMerchantId(),
                            e.getRefundId(),
                            "FAILED",
                            e.getReason(),
                            null);
            case ReservationReleased e ->
                    build(
                            e.getEnvelope(),
                            e.getPaymentId(),
                            e.getMerchantId(),
                            e.getRefundId(),
                            "RELEASED",
                            e.getReason(),
                            null);
            default ->
                    throw new IllegalArgumentException(
                            "Unrecognized realtime event type: " + avroEvent.getClass().getName());
        };
    }

    private RealtimeEvent build(
            EventEnvelope envelope,
            String paymentId,
            String merchantId,
            String refundId,
            String status,
            String reason,
            String providerReference) {
        return new RealtimeEvent(
                envelope.getEventId(),
                envelope.getEventType(),
                envelope.getOccurredAt(),
                paymentId,
                merchantId,
                refundId,
                status,
                reason,
                providerReference);
    }
}
