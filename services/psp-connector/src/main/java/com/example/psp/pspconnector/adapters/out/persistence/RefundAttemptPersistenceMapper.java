package com.example.psp.pspconnector.adapters.out.persistence;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.RefundAttempt;
import com.example.psp.pspconnector.domain.model.RefundOutcome;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * MapStruct mapper at the refund-path persistence hexagon boundary (M11, ADR-0007).
 * {@code componentModel = "spring"}, {@code unmappedTargetPolicy = ERROR} - same rule as every
 * other boundary mapper in this codebase.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundAttemptPersistenceMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    @Mapping(target = "outcome", expression = "java(attempt.getOutcome().name())")
    RefundAttemptEntity toEntity(RefundAttempt attempt);

    /**
     * Hand-written for the same reason as {@code PaymentAttemptPersistenceMapper#toDomain}:
     * {@link RefundAttempt} exposes only factories, nothing MapStruct's bean strategy can target.
     * First needed by the M19 drill 9 fix's republish-on-redelivery read path.
     */
    default RefundAttempt toDomain(RefundAttemptEntity entity) {
        if (entity == null) {
            return null;
        }
        return RefundAttempt.reconstitute(
                entity.getId(),
                entity.getRefundId(),
                entity.getPaymentId(),
                entity.getMerchantId(),
                Money.of(entity.getAmount(), entity.getCurrency()),
                entity.getProviderReference(),
                RefundOutcome.valueOf(entity.getOutcome()),
                entity.getProviderLatencyMs(),
                entity.getCausationEventId(),
                entity.getStatusEventId(),
                entity.getTraceId(),
                entity.getCorrelationId(),
                entity.getProcessedAt());
    }
}
