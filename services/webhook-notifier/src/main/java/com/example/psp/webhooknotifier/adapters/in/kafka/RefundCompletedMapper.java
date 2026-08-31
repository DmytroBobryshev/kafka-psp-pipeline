package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundCompleted;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the M19 refund planner's inbound Kafka boundary: event -&gt; domain command
 * (ADR-0007), for {@code refunds.refund-completed.v1}. Same shape as
 * {@code PaymentStatusChangedMapper} - {@code componentModel = "spring"},
 * {@code unmappedTargetPolicy = ERROR}, explicit {@code UUID.fromString(...)} expressions for the
 * event's string-typed UUID fields.
 *
 * <p>{@code status = "COMPLETED"}: {@code refunds.refund-completed.v1} carries no status field of
 * its own (its very existence on this topic IS the outcome - see that schema's doc), so the
 * constant echoes the event's own name, matching this record's existing "status is the source
 * event's own outcome vocabulary" convention (see {@link WebhookDeliveryCommand}'s javadoc).
 * {@code declineReason} stays {@code null}: a completed refund has nothing to explain.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundCompletedMapper {

    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "refundId", expression = "java(java.util.UUID.fromString(event.getRefundId()))")
    @Mapping(target = "status", constant = "COMPLETED")
    @Mapping(target = "declineReason", ignore = true)
    @Mapping(
            target = "causationEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    @Mapping(target = "eventType", constant = "REFUND_COMPLETED")
    WebhookDeliveryCommand toCommand(RefundCompleted event);
}
