package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentStatusChanged;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the planner's inbound Kafka boundary: event -&gt; domain command (ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - the same rule as
 * every other boundary mapper in this codebase.
 *
 * <p>M9 Phase 2: {@code event} is now the generated Avro {@link PaymentStatusChanged} record,
 * whose UUID-shaped fields are plain Avro {@code string} - hence the explicit
 * {@code UUID.fromString(...)} expressions below, matching the pattern psp-connector's
 * {@code PaymentRequestedMapper} established in M9 Phase 1 and ledger's own
 * {@code PaymentStatusChangedMapper} just adopted for the same topic.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusChangedMapper {

    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(
            target = "causationEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    // M19: distinguishes this planner source from the two refund ones - see
    // adapters.in.kafka.RefundCompletedMapper/RefundFailedMapper.
    @Mapping(target = "eventType", constant = "PAYMENT_STATUS_CHANGED")
    @Mapping(target = "refundId", ignore = true)
    WebhookDeliveryCommand toCommand(PaymentStatusChanged event);
}
