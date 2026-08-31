package com.example.psp.webhooknotifier.adapters.in.kafka;

import com.example.psp.common.events.avro.RefundFailed;
import com.example.psp.webhooknotifier.domain.model.WebhookDeliveryCommand;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the M19 refund planner's inbound Kafka boundary: event -&gt; domain command
 * (ADR-0007), for {@code refunds.refund-failed.v1}. Same shape as {@link RefundCompletedMapper}.
 *
 * <p>{@code refunds.refund-failed.v1} is produced by TWO different services at two different saga
 * decision points (the ledger, on insufficient balance; psp-connector, on a provider decline - see
 * that schema's doc) but this mapper does not need to know which: both producers emit the exact
 * same event shape, and {@code event.getReason()} ({@code "INSUFFICIENT_BALANCE"} or
 * {@code "PROVIDER_DECLINED"}) already carries the distinction a merchant would want, mapped onto
 * {@link WebhookDeliveryCommand#declineReason()} - the same slot a payment decline uses, per that
 * field's widened javadoc.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundFailedMapper {

    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "refundId", expression = "java(java.util.UUID.fromString(event.getRefundId()))")
    @Mapping(target = "status", constant = "FAILED")
    @Mapping(target = "declineReason", source = "event.reason")
    @Mapping(
            target = "causationEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    @Mapping(target = "eventType", constant = "REFUND_FAILED")
    WebhookDeliveryCommand toCommand(RefundFailed event);
}
