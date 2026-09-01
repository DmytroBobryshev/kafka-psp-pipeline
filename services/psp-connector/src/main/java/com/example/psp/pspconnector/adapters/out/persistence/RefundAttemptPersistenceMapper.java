package com.example.psp.pspconnector.adapters.out.persistence;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.RefundAttempt;
import com.example.psp.pspconnector.domain.model.RefundOutcome;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundAttemptPersistenceMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    @Mapping(target = "outcome", expression = "java(attempt.getOutcome().name())")
    RefundAttemptEntity toEntity(RefundAttempt attempt);

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
