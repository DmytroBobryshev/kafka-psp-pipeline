package com.example.psp.paymentapi.adapters.out.kafka;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Payment;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the outbound Kafka hexagon boundary: domain -&gt; event (ADR-0007). The
 * {@link EventEnvelope} is built by the caller ({@link KafkaPaymentEventPublisher}) - it needs
 * runtime context (trace id, correlation id) that has no source on {@link Payment} - and handed
 * in as a second source parameter; MapStruct maps it straight through to the {@code envelope}
 * target property because the parameter name and type match exactly.
 *
 * <p>{@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as
 * every other boundary mapper in this service.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentEventMapper {

    @Mapping(target = "envelope", source = "envelope")
    @Mapping(target = "paymentId", source = "payment.id")
    @Mapping(target = "merchantId", source = "payment.merchantId")
    @Mapping(target = "amount", source = "payment.amount.amount")
    @Mapping(target = "currency", source = "payment.amount.currency")
    @Mapping(target = "status", expression = "java(payment.getStatus().name())")
    PaymentRequested toEvent(EventEnvelope envelope, Payment payment);
}
