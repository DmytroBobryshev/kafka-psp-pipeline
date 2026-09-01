package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundStatusChanged;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the M24 refund-expiry planner's inbound Kafka boundary: event -&gt; domain
 * command (ADR-0007), for {@code refunds.refund-status-changed.v1}'s {@code EXPIRED} case only -
 * {@link RefundExpiredListener} is what filters to that one status before this mapper ever runs,
 * same shape as {@link RefundCompletedMapper}/{@link RefundFailedMapper} for the other two refund
 * planner sources.
 *
 * <p>{@code status = "EXPIRED"}: mirrors the event's own {@code status} field value (this mapper is
 * only ever invoked for that one value), matching this record's existing "status is the source
 * event's own outcome vocabulary" convention (see {@link WebhookDeliveryCommand}'s javadoc).
 * {@code declineReason} stays {@code null}: an expiry is payment-api's own sweep verdict, not a
 * provider-reported failure with a reason to explain.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundExpiredMapper {

    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "refundId", expression = "java(java.util.UUID.fromString(event.getRefundId()))")
    @Mapping(target = "status", constant = "EXPIRED")
    @Mapping(target = "declineReason", ignore = true)
    @Mapping(
            target = "causationEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    @Mapping(target = "eventType", constant = "REFUND_EXPIRED")
    WebhookDeliveryCommand toCommand(RefundStatusChanged event);
}
