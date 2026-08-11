package com.example.psp.ledger.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.ledger.domain.model.RefundRequest;
import com.example.psp.ledger.domain.model.RefundSagaState;
import org.springframework.stereotype.Component;

/**
 * Builds the Avro wire records for the ledger's three M11 refund-saga publications
 * ({@code refunds.funds-reserved.v1}, {@code refunds.refund-failed.v1},
 * {@code refunds.reservation-released.v1}) from a hand-written {@link EventEnvelope} plus the
 * relevant domain type. A plain class with three methods, not a MapStruct {@code @Mapper} - same
 * established exception as {@code LedgerEntryAvroEventFactory} (M9 Phase 2) and every other
 * *AvroEventFactory in this codebase: each target is a small flat record reached through the
 * generated {@code newBuilder()}/{@code build()}, not worth an annotation-processed interface.
 */
@Component
public class RefundEventAvroFactory {

    private static com.example.psp.common.events.avro.EventEnvelope toAvroEnvelope(EventEnvelope envelope) {
        return com.example.psp.common.events.avro.EventEnvelope.newBuilder()
                .setEventId(envelope.eventId().toString())
                .setEventType(envelope.eventType())
                .setEventVersion(envelope.eventVersion())
                .setAggregateId(envelope.aggregateId())
                .setAggregateType(envelope.aggregateType())
                .setOccurredAt(envelope.occurredAt())
                .setSource(envelope.source())
                .setTraceId(envelope.traceId())
                .setCorrelationId(envelope.correlationId())
                .setCausationId(envelope.causationId() == null ? null : envelope.causationId().toString())
                .build();
    }

    public com.example.psp.common.events.avro.FundsReserved toFundsReserved(
            EventEnvelope envelope, RefundRequest request) {
        return com.example.psp.common.events.avro.FundsReserved.newBuilder()
                .setEnvelope(toAvroEnvelope(envelope))
                .setRefundId(request.refundId().toString())
                .setPaymentId(request.paymentId().toString())
                .setMerchantId(request.merchantId())
                .setAmount(request.amount().amount())
                .setCurrency(request.amount().currency())
                .build();
    }

    public com.example.psp.common.events.avro.RefundFailed toRefundFailed(
            EventEnvelope envelope, RefundRequest request, String reason) {
        return com.example.psp.common.events.avro.RefundFailed.newBuilder()
                .setEnvelope(toAvroEnvelope(envelope))
                .setRefundId(request.refundId().toString())
                .setPaymentId(request.paymentId().toString())
                .setMerchantId(request.merchantId())
                .setAmount(request.amount().amount())
                .setCurrency(request.amount().currency())
                .setReason(reason)
                .build();
    }

    public com.example.psp.common.events.avro.ReservationReleased toReservationReleased(
            EventEnvelope envelope, RefundSagaState state, String reason) {
        return com.example.psp.common.events.avro.ReservationReleased.newBuilder()
                .setEnvelope(toAvroEnvelope(envelope))
                .setRefundId(state.refundId().toString())
                .setPaymentId(state.paymentId().toString())
                .setMerchantId(state.merchantId())
                .setAmount(state.amount().amount())
                .setCurrency(state.amount().currency())
                .setReason(reason)
                .build();
    }
}
