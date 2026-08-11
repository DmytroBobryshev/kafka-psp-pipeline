package com.example.psp.pspconnector.adapters.out.persistence;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the persistence hexagon boundary: {@link PaymentAttempt} -&gt;
 * {@link PaymentAttemptEntity} (ADR-0007). {@code componentModel = "spring"},
 * {@code unmappedTargetPolicy = ERROR} - same rule as every other boundary mapper in this
 * codebase.
 *
 * <p>M4 through M11 only ever wrote this table ("just record attempts" - see
 * {@link PaymentAttempt}'s javadoc), so there was no {@code toDomain} here. M12 needs one for its
 * read path ({@code domain.port.AttemptLogRepository#findLatestByPaymentId}, the request-reply
 * responder's status lookup) - a hand-written default method, same pattern as {@code payment-api}'s
 * {@code PaymentPersistenceMapper#toDomain}: {@link PaymentAttempt} has no public constructor or
 * setters (only {@code from}/{@code reconstitute} factories), so MapStruct's default bean-mapping
 * strategy has nothing to target automatically.
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

    default PaymentAttempt toDomain(PaymentAttemptEntity entity) {
        if (entity == null) {
            return null;
        }
        return PaymentAttempt.reconstitute(
                entity.getId(),
                entity.getPaymentId(),
                entity.getMerchantId(),
                Money.of(entity.getAmount(), entity.getCurrency()),
                entity.getProviderEventId(),
                ProviderOutcome.valueOf(entity.getOutcome()),
                entity.getProviderLatencyMs(),
                entity.getCausationEventId(),
                entity.getTraceId(),
                entity.getCorrelationId(),
                entity.getProcessedAt());
    }
}
