package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.MerchantConfig;
import org.springframework.stereotype.Component;

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
                .setRefundExpirationSeconds(config.refundExpirationSeconds())
                .build();
    }
}
