package com.example.psp.paymentapi.adapters.out.persistence;

import com.example.psp.paymentapi.domain.model.Money;
import com.example.psp.paymentapi.domain.model.Payment;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface PaymentPersistenceMapper {

    @Mapping(target = "amount", source = "amount.amount")
    @Mapping(target = "currency", source = "amount.currency")
    PaymentEntity toEntity(Payment payment);

    default Payment toDomain(PaymentEntity entity) {
        if (entity == null) {
            return null;
        }
        return Payment.reconstitute(
                entity.getId(),
                entity.getMerchantId(),
                Money.of(entity.getAmount(), entity.getCurrency()),
                entity.getStatus(),
                entity.getCreatedAt(),
                entity.getStatusUpdatedAt());
    }
}
