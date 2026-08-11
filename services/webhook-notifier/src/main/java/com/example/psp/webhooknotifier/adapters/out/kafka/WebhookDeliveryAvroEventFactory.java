package com.example.psp.webhooknotifier.adapters.out.kafka;

import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.springframework.stereotype.Component;

/**
 * Builds the Avro wire record for {@code webhooks.webhook-delivery-requested.v2} and its three
 * retry tiers (M9 Phase 2) from a {@link WebhookDeliveryCommand}.
 *
 * <p>A plain method, not a MapStruct {@code @Mapper} - the same deliberate exception
 * {@code payment-api}'s {@code PaymentAvroEventFactory} (M9 Phase 1) established, followed by
 * every other Avro construction point added in M9 Phase 2 ({@code psp-connector}'s
 * {@code PaymentStatusAvroEventFactory}, {@code ledger}'s {@code LedgerEntryAvroEventFactory}).
 *
 * <p>Deliberately separate from {@link WebhookDeliveryEventMapper}, which still targets the
 * hand-written, JSON-serialized {@code WebhookDeliveryRequested} record used ONLY for the terminal
 * {@code .dlq} topic - see {@code KafkaWebhookDeliveryPublisher}'s javadoc for why the DLQ stays on
 * a byte-tolerant JSON serializer while every other hop of the chain moves to Avro.
 */
@Component
public class WebhookDeliveryAvroEventFactory {

    public com.example.psp.common.events.avro.WebhookDeliveryRequested toAvro(WebhookDeliveryCommand command) {
        return com.example.psp.common.events.avro.WebhookDeliveryRequested.newBuilder()
                .setPaymentId(command.paymentId().toString())
                .setMerchantId(command.merchantId())
                .setAmount(command.amount())
                .setCurrency(command.currency())
                .setStatus(command.status())
                .setDeclineReason(command.declineReason())
                .setCausationEventId(command.causationEventId().toString())
                .setTraceId(command.traceId())
                .setCorrelationId(command.correlationId())
                .build();
    }
}
