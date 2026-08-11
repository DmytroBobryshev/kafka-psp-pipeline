package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.common.events.avro.ProviderStatusQuery;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import java.time.Instant;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * Builds the {@code psp.provider-status-reply.v1} Avro wire record (M12) from the outcome of
 * {@code application.CheckProviderStatusUseCase} plus the inbound {@link ProviderStatusQuery} it
 * is replying to. A plain method, not a MapStruct {@code @Mapper} - the same established exception
 * every other {@code *AvroEventFactory} in this codebase uses (see
 * {@code PaymentStatusAvroEventFactory}'s javadoc for the reasoning).
 */
@Component
public class ProviderStatusAvroEventFactory {

    private static final String EVENT_TYPE = "psp.provider-status-reply.v1";
    private static final String SOURCE = "psp-connector";
    private static final String AGGREGATE_TYPE = "payment";

    public com.example.psp.common.events.avro.ProviderStatusReply toReply(
            ProviderStatusQuery query, Optional<PaymentAttempt> latestAttempt, String traceId, String correlationId) {
        EventEnvelope replyEnvelope =
                EventEnvelope.causedBy(
                        java.util.UUID.fromString(query.getEnvelope().getEventId()),
                        EVENT_TYPE,
                        1,
                        query.getPaymentId(),
                        AGGREGATE_TYPE,
                        SOURCE,
                        traceId,
                        correlationId);

        com.example.psp.common.events.avro.EventEnvelope avroEnvelope =
                com.example.psp.common.events.avro.EventEnvelope.newBuilder()
                        .setEventId(replyEnvelope.eventId().toString())
                        .setEventType(replyEnvelope.eventType())
                        .setEventVersion(replyEnvelope.eventVersion())
                        .setAggregateId(replyEnvelope.aggregateId())
                        .setAggregateType(replyEnvelope.aggregateType())
                        .setOccurredAt(replyEnvelope.occurredAt())
                        .setSource(replyEnvelope.source())
                        .setTraceId(replyEnvelope.traceId())
                        .setCorrelationId(replyEnvelope.correlationId())
                        .setCausationId(
                                replyEnvelope.causationId() == null ? null : replyEnvelope.causationId().toString())
                        .build();

        var builder =
                com.example.psp.common.events.avro.ProviderStatusReply.newBuilder()
                        .setEnvelope(avroEnvelope)
                        .setPaymentId(query.getPaymentId())
                        .setMerchantId(query.getMerchantId())
                        .setCheckedAt(Instant.now());

        if (latestAttempt.isPresent()) {
            PaymentAttempt attempt = latestAttempt.get();
            builder.setFound(true)
                    .setStatus(attempt.getOutcome().name())
                    .setProviderReference(attempt.getProviderEventId().toString());
        } else {
            builder.setFound(false).setStatus(null).setProviderReference(null);
        }

        return builder.build();
    }
}
