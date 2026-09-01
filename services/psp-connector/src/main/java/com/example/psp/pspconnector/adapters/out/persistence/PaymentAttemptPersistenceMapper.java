package com.example.psp.pspconnector.adapters.out.persistence;

import com.example.psp.pspconnector.domain.model.Money;
import com.example.psp.pspconnector.domain.model.PaymentAttempt;
import com.example.psp.pspconnector.domain.model.ProviderOutcome;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

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
                entity.getStatusEventId(),
                entity.getTraceId(),
                entity.getCorrelationId(),
                entity.getProcessedAt());
    }
}
