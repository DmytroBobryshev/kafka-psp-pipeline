package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.MerchantConfig;
import org.springframework.stereotype.Component;

/**
 * Builds the Avro wire record for {@code merchants.merchant-config-changed.v1} (M10) from the
 * hand-written {@link EventEnvelope} and the {@link MerchantConfig} aggregate.
 *
 * <p>A plain method, not a MapStruct {@code @Mapper}, for exactly the reasons
 * {@code adapters.out.outbox.PaymentAvroEventFactory}'s javadoc lays out for the M9 Phase 1
 * equivalent - a flat record reached through the generated Avro builder with one real conversion
 * (UUID {@code ->} String) is not worth an annotation-processed interface.
 *
 * <p>There is no {@code toAvro} overload for the delete case, and that absence is the point: a
 * tombstone has <b>no Avro record at all</b>. Its value is {@code null} on the wire - no magic
 * byte, no schema id, zero bytes - so it never touches this class, never touches Schema Registry,
 * and is not validated against any schema. See
 * {@link com.example.psp.paymentapi.domain.port.MerchantConfigPublisher#publishConfigDeleted}.
 */
@Component
public class MerchantConfigAvroEventFactory {

    public com.example.psp.common.events.avro.MerchantConfigChanged toAvro(
            EventEnvelope envelope, MerchantConfig config) {

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

        return com.example.psp.common.events.avro.MerchantConfigChanged.newBuilder()
                .setEnvelope(avroEnvelope)
                .setMerchantId(config.merchantId())
                .setDisplayName(config.displayName())
                .setStatus(config.status().name())
                .setPayoutCurrency(config.payoutCurrency())
                .setAllowedCurrencies(config.allowedCurrencies())
                .setWebhookUrl(config.webhookUrl())
                .setDeclineRateAlertThresholdBps(config.declineRateAlertThresholdBps())
                .setPaymentExpirationSeconds(config.paymentExpirationSeconds())
                .build();
    }
}
