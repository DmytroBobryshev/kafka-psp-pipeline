package com.example.psp.paymentapi.adapters.out.outbox;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Payment;
import org.springframework.stereotype.Component;

/**
 * Builds the Avro wire record for {@code payments.payment-requested.v1} (M9 Phase 1) from the
 * hand-written {@link EventEnvelope} and the {@link Payment} aggregate.
 *
 * <p>A plain method, not a MapStruct {@code @Mapper}, on purpose - the one deliberate exception to
 * this codebase's "MapStruct at every boundary" convention (PLAN.md). MapStruct does support the
 * builder pattern the generated Avro classes use ({@code newBuilder()}/{@code build()}), but the
 * target here is a five-field flat record reached through that builder with exactly two real type
 * conversions (UUID {@code ->} String for {@code eventId}/{@code causationId}; everything else is
 * a same-type field copy since the Avro schema's {@code occurredAt} and {@code amount} logical
 * types were chosen specifically to generate {@link java.time.Instant} and
 * {@link java.math.BigDecimal} - see {@code libs/common-events/src/main/avro} - matching the
 * domain types with zero conversion). Wiring an annotation-processed interface around two
 * one-line conversions adds indirection without reducing risk or code size, and a plain method is
 * easier to read when a future phase changes the generated Avro shape.
 */
@Component
public class PaymentAvroEventFactory {

    /**
     * @param envelope the JSON-era {@link EventEnvelope} payment-api already builds for every
     *                 outbound event (unchanged - still the source of the outbox row's primary
     *                 key / Debezium event id, see {@link OutboxPaymentEventPublisher}).
     * @param payment  the aggregate that was just persisted.
     * @return the Avro record ready to hand to {@code KafkaAvroSerializer#serialize}.
     */
    public com.example.psp.common.events.avro.PaymentRequested toAvro(EventEnvelope envelope, Payment payment) {
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
                        .setCausationId(envelope.causationId() == null ? null : envelope.causationId().toString())
                        .build();

        return com.example.psp.common.events.avro.PaymentRequested.newBuilder()
                .setEnvelope(avroEnvelope)
                .setPaymentId(payment.getId().toString())
                .setMerchantId(payment.getMerchantId())
                .setAmount(payment.getAmount().amount())
                .setCurrency(payment.getAmount().currency())
                .setStatus(payment.getStatus().name())
                .build();
    }
}
