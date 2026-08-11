package com.example.psp.paymentapi.adapters.out.outbox;

import com.example.psp.common.events.EventEnvelope;
import com.example.psp.paymentapi.domain.model.Payment;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the outbound outbox hexagon boundary: domain -&gt; payload (ADR-0007), the
 * outbox-boundary twin of {@code adapters.out.kafka.PaymentEventMapper}. Same shape, same
 * reasoning: the {@link EventEnvelope} is built by the caller ({@link OutboxPaymentEventPublisher})
 * and handed in as a second source parameter since it needs runtime context (trace id,
 * correlation id) that {@link Payment} has no source for.
 *
 * <p><b>Retired as of M9 Phase 1</b>, same convention as {@code adapters.out.kafka.*} (M3): kept
 * for reference, no longer called by {@link OutboxPaymentEventPublisher}, which now builds the
 * generated Avro record via {@link PaymentAvroEventFactory} instead of this JSON-era
 * {@link PaymentRequestedOutboxPayload} shape - see that class's javadoc for why.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentOutboxEventMapper {

    @Mapping(target = "envelope", source = "envelope")
    @Mapping(target = "paymentId", source = "payment.id")
    @Mapping(target = "merchantId", source = "payment.merchantId")
    @Mapping(target = "amount", source = "payment.amount.amount")
    @Mapping(target = "currency", source = "payment.amount.currency")
    @Mapping(target = "status", expression = "java(payment.getStatus().name())")
    PaymentRequestedOutboxPayload toPayload(EventEnvelope envelope, Payment payment);
}
