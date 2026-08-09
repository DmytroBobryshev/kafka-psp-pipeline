package com.example.psp.pspconnector.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the outbound Kafka hexagon boundary: domain -&gt; event (ADR-0007). The
 * {@link EventEnvelope} is built by the caller ({@link KafkaPaymentStatusPublisher}) - same
 * pattern as {@code payment-api}'s {@code PaymentEventMapper}: it needs runtime causation context
 * that has no source on {@link PaymentAttempt} alone (the causing event id), so it's handed in as
 * a second source parameter and mapped straight through by matching parameter name/type.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentStatusEventMapper {

    @Mapping(target = "envelope", source = "envelope")
    @Mapping(target = "paymentId", source = "attempt.paymentId")
    @Mapping(target = "merchantId", source = "attempt.merchantId")
    @Mapping(target = "amount", source = "attempt.amount.amount")
    @Mapping(target = "currency", source = "attempt.amount.currency")
    @Mapping(
            target = "status",
            expression =
                    "java(attempt.getOutcome() == com.example.psp.pspconnector.domain.model.ProviderOutcome.APPROVED"
                            + " ? \"SUCCEEDED\" : \"DECLINED\")")
    @Mapping(target = "providerReference", source = "attempt.providerEventId")
    @Mapping(
            target = "declineReason",
            expression =
                    "java(attempt.getOutcome() == com.example.psp.pspconnector.domain.model.ProviderOutcome.DECLINED"
                            + " ? \"simulated decline\" : null)")
    PaymentStatusChanged toEvent(EventEnvelope envelope, PaymentAttempt attempt);
}
