package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.DocumentReference;
import java.nio.ByteBuffer;
import org.springframework.stereotype.Component;

/**
 * Builds the Avro wire record for {@code disputes.dispute-opened.v1} (M13) from the hand-written
 * {@link EventEnvelope} and the dispute fields. A plain method pair, not a MapStruct
 * {@code @Mapper} - same established exception as {@link MerchantConfigAvroEventFactory}.
 *
 * <p>Two build methods, {@link #toInlineAvro} and {@link #toClaimCheckAvro}, mirroring
 * {@code domain.port.DisputeEventPublisher}'s two publish methods one level down: each builds the
 * {@code DisputeOpened} envelope once and sets exactly one branch of the {@code document} union
 * (generated as {@code Object} - see either method body), never both.
 */
@Component
public class DisputeAvroEventFactory {

    public com.example.psp.common.events.avro.DisputeOpened toInlineAvro(
            EventEnvelope envelope,
            String disputeId,
            String paymentId,
            String merchantId,
            String reason,
            byte[] documentBytes,
            String contentType) {

        com.example.psp.common.events.avro.InlineDocument inline =
                com.example.psp.common.events.avro.InlineDocument.newBuilder()
                        .setContentType(contentType)
                        .setSizeBytes(documentBytes.length)
                        .setBytes(ByteBuffer.wrap(documentBytes))
                        .build();

        return baseBuilder(envelope, disputeId, paymentId, merchantId, reason).setDocument(inline).build();
    }

    public com.example.psp.common.events.avro.DisputeOpened toClaimCheckAvro(
            EventEnvelope envelope, String disputeId, String paymentId, String merchantId, String reason,
            DocumentReference reference) {

        com.example.psp.common.events.avro.ClaimCheckReference claimCheck =
                com.example.psp.common.events.avro.ClaimCheckReference.newBuilder()
                        .setBucket(reference.bucket())
                        .setObjectKey(reference.objectKey())
                        .setSizeBytes(reference.sizeBytes())
                        .setContentType(reference.contentType())
                        .build();

        return baseBuilder(envelope, disputeId, paymentId, merchantId, reason)
                .setDocument(claimCheck)
                .build();
    }

    private com.example.psp.common.events.avro.DisputeOpened.Builder baseBuilder(
            EventEnvelope envelope, String disputeId, String paymentId, String merchantId, String reason) {
        com.example.psp.common.events.avro.EventEnvelope avroEnvelope =
                com.example.psp.common.events.avro.EventEnvelope.newBuilder()
                        .setEventId(envelope.eventId().toString())
                        .setEventType(envelope.eventType())
                        .setEventVersion(envelope.eventVersion())
                        .setAggregateId(envelope.aggregateId())
                        .setAggregateType(envelope.aggregateType())
                        .setOccurredAt(envelope.occurredAt())
                        .setSource(envelope.source())
                        .setTraceId(envelope.traceId())
                        .setCorrelationId(envelope.correlationId())
                        .setCausationId(
                                envelope.causationId() == null ? null : envelope.causationId().toString())
                        .build();

        return com.example.psp.common.events.avro.DisputeOpened.newBuilder()
                .setEnvelope(avroEnvelope)
                .setDisputeId(disputeId)
                .setPaymentId(paymentId)
                .setMerchantId(merchantId)
                .setReason(reason);
    }
}
