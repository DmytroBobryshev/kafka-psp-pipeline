package com.example.psp.ledger.adapters.out.persistence;

import com.example.psp.ledger.domain.model.Money;
import com.example.psp.ledger.domain.model.RefundReservation;
import com.example.psp.ledger.domain.model.RefundSagaState;
import com.example.psp.ledger.domain.model.RefundSagaStatus;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RefundPersistenceMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    RefundReservationEntity toEntity(RefundReservation reservation);

    default RefundSagaState toDomain(RefundSagaStateEntity entity) {
        return new RefundSagaState(
                entity.getRefundId(),
                entity.getPaymentId(),
                entity.getMerchantId(),
                Money.of(entity.getAmount(), entity.getCurrency()),
                RefundSagaStatus.valueOf(entity.getStatus()),
                entity.getReason(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }
}
