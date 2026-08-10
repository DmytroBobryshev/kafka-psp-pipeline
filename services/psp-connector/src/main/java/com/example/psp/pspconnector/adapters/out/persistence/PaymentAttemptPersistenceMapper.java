package com.example.psp.pspconnector.adapters.out.persistence;

import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the persistence hexagon boundary: {@link PaymentAttempt} -&gt;
 * {@link PaymentAttemptEntity} (ADR-0007). {@code componentModel = "spring"},
 * {@code unmappedTargetPolicy = ERROR} - same rule as every other boundary mapper in this
 * codebase.
 *
 * <p>M4 only ever writes this table ("just record attempts" - see {@link PaymentAttempt}'s
 * javadoc), so unlike {@code payment-api}'s equivalent mapper there is no hand-written
 * {@code toDomain} default method here yet; M5's real idempotency check (query-before-call) is
 * what will need one.
 *
 * <p>{@code inboundEventId} has no matching {@link PaymentAttempt} field of the same name - it is
 * deliberately mapped from {@code causationEventId} (see {@link PaymentAttemptEntity}'s field
 * comment for why the same value lands in two separate columns).
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentAttemptPersistenceMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    @Mapping(target = "outcome", expression = "java(attempt.getOutcome().name())")
    @Mapping(target = "inboundEventId", source = "causationEventId")
    PaymentAttemptEntity toEntity(PaymentAttempt attempt);
}
