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
                str(envelope.getCausationId()),
                str(envelope.getSource()),
                paymentId,
                merchantId,
                refundId,
                status,
                reason,
                providerReference);
    }

    private static String str(CharSequence value) {
        return value == null ? null : value.toString();
    }
}
