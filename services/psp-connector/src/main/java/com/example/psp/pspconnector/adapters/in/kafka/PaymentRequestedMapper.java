package com.example.psp.pspconnector.adapters.in.kafka;

import com.example.psp.common.events.avro.PaymentRequested;
import com.example.psp.pspconnector.application.ProcessPaymentRequestCommand;
import com.example.psp.pspconnector.domain.model.Money;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the inbound Kafka hexagon boundary: event -&gt; command (ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase.
 *
 * <p>M9 Phase 1: {@link PaymentRequestedListener} (the production listener) now consumes the
 * generated Avro {@link PaymentRequested} record, whose UUID-shaped fields ({@code paymentId},
 * {@code envelope.eventId}) are plain Avro {@code string} (see
 * {@code libs/common-events/src/main/avro}) - hence the explicit {@code UUID.fromString(...)}
 * expressions on the first overload below, where {@code paymentId} needed none for the JSON-era
 * {@code PaymentRequestedEvent} overload (its {@code paymentId} was already a {@code UUID}). Both
 * overloads are kept: the second still backs the {@code auto-commit-drill} profile's
 * {@code AutoCommitDriftListener}, which stayed on the JSON-era shape - see that class's javadoc
 * for why.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentRequestedMapper {

    @Mapping(target = "paymentId", expression = "java(java.util.UUID.fromString(event.getPaymentId()))")
    @Mapping(target = "amount", expression = "java(Money.of(event.getAmount(), event.getCurrency()))")
    @Mapping(
            target = "causationEventId",
            expression = "java(java.util.UUID.fromString(event.getEnvelope().getEventId()))")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    ProcessPaymentRequestCommand toCommand(PaymentRequested event);

    /**
     * JSON-era overload, kept only for the {@code auto-commit-drill} profile (see class javadoc) -
     * field-for-field identical to the mapping this interface had before M9 Phase 1.
     */
    @Mapping(target = "amount", expression = "java(Money.of(event.amount(), event.currency()))")
    @Mapping(target = "causationEventId", source = "event.envelope.eventId")
    @Mapping(target = "traceId", source = "event.envelope.traceId")
    @Mapping(target = "correlationId", source = "event.envelope.correlationId")
    ProcessPaymentRequestCommand toCommand(PaymentRequestedEvent event);
}
